
#[path = "../../cufflink-fn/tests/library_example/mod.rs"]
mod library_example;

cufflink_fn::init!();

cufflink_fn::handler!(create_loan, library_example::create_loan);
cufflink_fn::handler!(loans_by_book, library_example::loans_by_book);
cufflink_fn::handler!(on_book_event, library_example::on_book_event);
cufflink_fn::handler!(delete_loan, library_example::delete_loan);
cufflink_fn::handler!(overdue_reminders, library_example::overdue_reminders);
cufflink_fn::handler!(send_overdue_reminder, library_example::send_overdue_reminder);
cufflink_fn::handler!(render_receipt_pdf, library_example::render_receipt_pdf);
