use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "todos", auth_required, owner_field = "user_id")]
pub struct Todo {
    #[key]
    pub id: Uuid,
    pub user_id: String,
    pub title: String,
    pub completed: bool,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "todo-service",
    tables: [Todo],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_field_is_threaded_through_macro() {
        let def = Todo::__cufflink_table_def();
        assert_eq!(def.owner_field.as_deref(), Some("user_id"));
        assert!(def.auth_required);
    }

    #[test]
    fn table_name_is_preserved() {
        let def = Todo::__cufflink_table_def();
        assert_eq!(def.name, "todos");
    }
}
