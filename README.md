# Migration

A golang library for running MySQL migrations.

## How To Use It

Define your migrations in code:

```
var migrations = []migration.Migration{
  &migration.Definition{
    ID: 1,
    UP: `CREATE TABLE blarg (id INT NOT NULL, PRIMARY KEY (id))`
  }
}
```

Then run them:

```
migration.MustMigrate(context.Background(), dbDSN, migrations)
```

Once run you can also dump the DB schema:

```
migration.MustDumpSchema(context.Background(), dbDSN, "/path/to/store/schemas")
```

Then use those schema definitions to setup your DB for testing!

```
migration.MustLoadSchema(context.Background(), dbDSN, "/path/to/store/schemas")
```

## Development

Still kinda sketchy, but there are tests:

```
docker-compose run --rm migration go test ./...
```
