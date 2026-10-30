use super::ResponseText;
use crate::tenant_sessions::{Flow, RowSink};
use crate::wasm_runtime::budget::{Hold, HostCaps, Refused};
use cufflink_pgcodec::render::{columns_v2, ColumnOrder};
use cufflink_types::db::{Column, DbError, DbErrorKind};
use std::sync::{Mutex, PoisonError};
use tokio_postgres::Row;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Legacy,
    V2,
}

#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum Stopped {
    Refused(Refused),
    Decode(DbError),
}

impl Stopped {
    pub fn error(&self) -> DbError {
        match self {
            Self::Refused(refused) => DbError::new(DbErrorKind::TooLarge, refused.to_string()),
            Self::Decode(error) => error.clone(),
        }
    }
}

struct State<'a> {
    text: ResponseText<'a>,
    order: Option<ColumnOrder>,
    columns: Vec<Column>,
    first: bool,
    stopped: Option<Stopped>,
}

#[doc(hidden)]
pub struct RowText<'a> {
    shape: Shape,
    state: Mutex<State<'a>>,
}

impl<'a> RowText<'a> {
    pub fn legacy(hold: &'a mut Hold, caps: &'a HostCaps, path: &'static str) -> Self {
        Self::new(Shape::Legacy, hold, caps, path)
    }

    pub fn v2(hold: &'a mut Hold, caps: &'a HostCaps, path: &'static str) -> Self {
        Self::new(Shape::V2, hold, caps, path)
    }

    fn new(shape: Shape, hold: &'a mut Hold, caps: &'a HostCaps, path: &'static str) -> Self {
        Self {
            shape,
            state: Mutex::new(State {
                text: ResponseText::new(hold, caps, path),
                order: None,
                columns: Vec::new(),
                first: true,
                stopped: None,
            }),
        }
    }

    pub fn stop(&self) -> Option<Stopped> {
        self.lock().stopped.clone()
    }

    pub fn refused(&self) -> Option<Refused> {
        match self.lock().stopped {
            Some(Stopped::Refused(refused)) => Some(refused),
            _ => None,
        }
    }

    pub fn finish(self) -> Result<String, Stopped> {
        let mut state = self
            .state
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(stopped) = state.stopped.take() {
            return Err(stopped);
        }
        let closing = match self.shape {
            Shape::Legacy => "]".to_owned(),
            Shape::V2 => {
                let columns = serde_json::to_string(&state.columns).map_err(|e| {
                    Stopped::Decode(DbError::new(DbErrorKind::Protocol, e.to_string()))
                })?;
                format!("],\"columns\":{columns}}}")
            }
        };
        state.text.push(&closing).map_err(Stopped::Refused)?;
        Ok(state.text.into_text())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State<'a>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn opening(&self) -> &'static str {
        match self.shape {
            Shape::Legacy => "[",
            Shape::V2 => "{\"ok\":true,\"rows\":[",
        }
    }
}

impl RowSink for RowText<'_> {
    fn restart(&self, columns: &[tokio_postgres::Column]) {
        let mut state = self.lock();
        state.text.clear();
        state.order = Some(ColumnOrder::of(columns));
        state.columns = match self.shape {
            Shape::Legacy => Vec::new(),
            Shape::V2 => columns_v2(columns),
        };
        state.first = true;
        state.stopped = state.text.push(self.opening()).err().map(Stopped::Refused);
    }

    fn rows(&self, rows: &[Row]) -> Flow {
        let mut state = self.lock();
        let State {
            text,
            order,
            first,
            stopped,
            ..
        } = &mut *state;
        match stopped {
            Some(Stopped::Refused(_)) => return Flow::Stop,
            Some(Stopped::Decode(_)) => return Flow::More,
            None => {}
        }
        let Some(order) = order else {
            return Flow::Stop;
        };
        let mut row_text = String::new();
        for row in rows {
            row_text.clear();
            if !std::mem::take(first) {
                row_text.push(',');
            }
            let rendered = match self.shape {
                Shape::Legacy => {
                    order.legacy_row(row, &mut row_text);
                    Ok(())
                }
                Shape::V2 => order.row_v2(row, &mut row_text),
            };
            let pushed = rendered
                .map_err(Stopped::Decode)
                .and_then(|()| text.push(&row_text).map_err(Stopped::Refused));
            if let Err(stop) = pushed {
                let flow = match stop {
                    Stopped::Refused(_) => Flow::Stop,
                    Stopped::Decode(_) => Flow::More,
                };
                *stopped = Some(stop);
                return flow;
            }
        }
        Flow::More
    }
}
