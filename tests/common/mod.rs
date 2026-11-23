//! The test server: `GNOMON_TEST_URL` (a superuser URL, trust auth), as `env.ts` does for the
//! TypeScript SDK. Without it the database tests are skipped, loudly.
#![allow(dead_code)]

use gnomon::Gnomon;
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, Connection, PgConnection, PgPool};

pub fn server() -> Option<String> {
    match std::env::var("GNOMON_TEST_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!(
                "SKIPPED: set GNOMON_TEST_URL to a superuser URL of a server with gnomon (see README)"
            );
            None
        }
    }
}

fn with_db(url: &str, db: &str, user: &str) -> String {
    let mut u = url::Url::parse(url).unwrap();
    u.set_path(db);
    u.set_username(user).unwrap();
    u.to_string()
}

async fn exec(c: &mut PgConnection, sql: &str) {
    sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .execute(&mut *c)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A fresh database with gnomon, an app login and a confined runs login (spec: Two logins).
pub struct Fixture {
    pub server: String,
    pub db: String,
    pub app_role: String,
    pub runs_role: String,
    pub cmod: bool,
    pub app: PgPool,
    pub runs: PgPool,
}

impl Fixture {
    pub async fn new(server: &str, name: &str, setup: &[String]) -> Fixture {
        let clean = |s: &str| {
            s.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() {
                        c.to_ascii_lowercase()
                    } else {
                        '_'
                    }
                })
                .collect::<String>()
        };
        let db: String = format!("sdk_rs_{}", clean(name)).chars().take(60).collect();
        let sfx: String = clean(name).chars().take(40).collect();
        let app_role = format!("sdk_rs_app_{sfx}");
        let runs_role = format!("sdk_rs_runs_{sfx}");
        let mut a = PgConnection::connect(&with_db(server, "postgres", "postgres"))
            .await
            .unwrap();
        exec(
            &mut a,
            &format!("drop database if exists {db} with (force)"),
        )
        .await;
        exec(&mut a, &format!("create database {db}")).await;
        for r in [&app_role, &runs_role] {
            let _ = sqlx::raw_sql(AssertSqlSafe(format!("drop role if exists {r}")))
                .execute(&mut a)
                .await;
            exec(&mut a, &format!("create role {r} login")).await;
        }
        a.close().await.unwrap();
        let mut c = PgConnection::connect(&with_db(server, &db, "postgres"))
            .await
            .unwrap();
        exec(
            &mut c,
            "set client_min_messages = warning; create extension gnomon",
        )
        .await;
        for s in setup {
            exec(&mut c, s).await;
        }
        exec(
            &mut c,
            &format!("grant gnomon_runner, gnomon_merger to {app_role}"),
        )
        .await;
        exec(
            &mut c,
            &format!(
                "grant select, insert, update, delete on all tables in schema public to {app_role}"
            ),
        )
        .await;
        exec(
            &mut c,
            &format!("grant usage on all sequences in schema public to {app_role}"),
        )
        .await;
        exec(
            &mut c,
            &format!("select gnomon.confine('{runs_role}', array[]::text[], false)"),
        )
        .await;
        exec(
            &mut c,
            &format!("select gnomon.set('runs_role', '{runs_role}')"),
        )
        .await;
        let cmod: bool = sqlx::query_scalar(
            "select coalesce(current_setting('gnomon.c_module', true), '') = 'on'",
        )
        .fetch_one(&mut c)
        .await
        .unwrap();
        c.close().await.unwrap();
        let pool = |user: &str| {
            PgPoolOptions::new()
                .max_connections(4)
                .connect_lazy(&with_db(server, &db, user))
                .unwrap()
        };
        Fixture {
            server: server.to_string(),
            app: pool(&app_role),
            runs: pool(&runs_role),
            db,
            app_role,
            runs_role,
            cmod,
        }
    }

    pub fn gnomon(&self) -> Gnomon {
        let mut base = url::Url::parse(&self.server).unwrap();
        base.set_path(&self.db);
        base.set_username("").unwrap();
        Gnomon::builder(self.app.clone())
            .runs(self.runs.clone())
            .url(base.to_string())
            .build()
    }

    /// A second application login, for a person who reviews what the first one opened.
    pub async fn reviewer(&self) -> Gnomon {
        let role = format!("{}_rev", self.app_role);
        let mut c = self.admin().await;
        let _ = sqlx::raw_sql(AssertSqlSafe(format!("drop role if exists {role}")))
            .execute(&mut c)
            .await;
        exec(
            &mut c,
            &format!("create role {role} login in role gnomon_runner, gnomon_merger"),
        )
        .await;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_lazy(&with_db(&self.server, &self.db, &role))
            .unwrap();
        Gnomon::builder(pool).build()
    }

    /// A superuser connection to the fixture's database.
    pub async fn admin(&self) -> PgConnection {
        PgConnection::connect(&with_db(&self.server, &self.db, "postgres"))
            .await
            .unwrap()
    }

    pub async fn cleanup(self) {
        self.app.close().await;
        self.runs.close().await;
        let mut x = PgConnection::connect(&with_db(&self.server, "postgres", "postgres"))
            .await
            .unwrap();
        exec(
            &mut x,
            &format!("drop database if exists {} with (force)", self.db),
        )
        .await;
        for r in [
            &self.app_role,
            &self.runs_role,
            &format!("{}_rev", self.app_role),
        ] {
            let _ = sqlx::raw_sql(AssertSqlSafe(format!("drop role if exists {r}")))
                .execute(&mut x)
                .await;
        }
    }
}
