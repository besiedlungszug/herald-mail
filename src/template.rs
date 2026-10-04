use std::ops::Deref;
use std::collections::HashMap;
use std::sync::Arc;

use handlebars::{Handlebars, DirectorySourceOptionsBuilder};
use tokio::sync::mpsc::{Sender as MpscSender, Receiver as MpscReceiver};

use herald::template;
use herald::types::contract::Contract;
use herald::sqlx::MySqlConnection;
use herald::Uuid;

pub(crate) async fn renderer(mut conn: MySqlConnection, mut rx: MpscReceiver<(Uuid, u64)>, tx: MpscSender<String>) -> () {
    let mut cache: HashMap<u64, Option<Arc<String>>> = HashMap::new();
    let mut handlebars = Handlebars::new();
    handlebars.register_templates_directory("srv/skel", DirectorySourceOptionsBuilder::default().tpl_extension(".hbs").build().unwrap()).expect("Error while reading template directory");
    handlebars.set_strict_mode(true);
    handlebars.register_escape_fn(handlebars::no_escape);
    let handlebars = Arc::new(handlebars);
    loop {
        let (registration_id, status) = rx.recv().await.expect("Error while reading queue 'upd': closed handle");
        if !cache.contains_key(&status) {
            let slug = template::query(&mut conn, status).await.expect("Error while fetching template slug");
            cache.insert(status, if let Some(slug) = slug { Some(Arc::new(slug)) } else { None });
        }
        let slug = cache.get(&status).expect("Error while reading template slug cache: cache empty");
        if let Some(slug) = slug {
            let contract = Contract::fetch(slug, &mut conn, registration_id).await.expect("Error while fetching contract data");
            let handlebars = handlebars.clone();
            let slug = slug.clone();
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || render(handlebars, slug, contract.into_inner(), tx));
        }
        //if let Err(err) = sqlx::query_file!("sql/registration_queue/delete.sql", registration_id, status).execute(&mut conn).await {
        //    eprintln!("Error while clearing registration queue: {}", err);
        //}
    }
}

fn render<'a>(handlebars: impl Deref<Target = Handlebars<'a>>, slug: impl Deref<Target: AsRef<str>>, contract: impl serde::Serialize, tx: MpscSender<String>) -> () {
    let message = handlebars.render(slug.as_ref(), &contract).expect("Error while rendering mail template");
    tx.blocking_send(message).expect("Error while writing queue 'rnd': closed handle");
}

#[cfg(test)]
mod tests {
    use super::render;

    use tokio::sync::mpsc;
    use handlebars::Handlebars;

    #[tokio::test]
    async fn handlebars_render_template() {
        let mut handlebars = Handlebars::new();
        let template = "Hello {{name}},\nwe are glad to confirm your registration to {{topic}}.";
        handlebars.register_template_string("test", template).unwrap();
        handlebars.set_strict_mode(true);
        handlebars.register_escape_fn(handlebars::no_escape);

        let (tx, mut rx) = mpsc::channel::<String>(1);
        #[derive(serde::Serialize)]
        struct Thing {
            name: String,
            topic: String,
        }
        let thing = Thing {
            name: "Testo Husbando".to_string(),
            topic: "Event 'Unittesting is great'".to_string(),
        };
        tokio::task::spawn_blocking(move || render(&handlebars, "test", thing, tx));
        let message = rx.recv().await.expect("handle closed prematurely");
        assert_eq!(message, "Hello Testo Husbando,\nwe are glad to confirm your registration to Event 'Unittesting is great'.");
    }
}
