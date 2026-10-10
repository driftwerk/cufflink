use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "posts")]
pub struct Post {
    #[key]
    pub id: Uuid,
    pub title: String,
    pub slug: Option<String>,
    pub body: String,
    pub author_id: Uuid,
    pub published: bool,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "comments")]
pub struct Comment {
    #[key]
    pub id: Uuid,
    #[references("posts.id")]
    #[on_delete("cascade")]
    pub post_id: Uuid,
    pub author_name: String,
    pub body: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "tags")]
pub struct Tag {
    #[key]
    pub id: Uuid,
    pub name: String,
    #[references("posts.id")]
    #[on_delete("cascade")]
    pub post_id: Uuid,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}

cufflink::service! {
    name: "blog-service",
    tables: [Post, Comment, Tag],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_field_defaults_to_none_when_not_declared() {
        assert!(Post::__cufflink_table_def().owner_field.is_none());
        assert!(Comment::__cufflink_table_def().owner_field.is_none());
        assert!(Tag::__cufflink_table_def().owner_field.is_none());
    }
}
