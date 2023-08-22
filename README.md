# sql2any
Export data from SQL databases in various formats.

Currently supported database drivers:

- PostgreSQL (via SQLx);
- MySQL (via SQLx);

Currently supported file formats:

- XLSX (via rust_xlsxwriter);
- JSON (via serde);
- GFM tables;

Development is in very early stage but could be useful for quick database export in XLSX and for embedding tables in markdown docs.

Planned file formats support:

- CSV;
- SQL "INSERT INTO";
- SQL "LOAD DATA INFILE" / "COPY FROM";
- Bincode;
- Apache Arrow IPC;

Planned database driver support:

- SQLite (via SQLx);
- PostgreSQL (via tokio-postgres);
- MySQL (via mysql_async);
- MSSQL (via tiberius);

## TODO
- [ ] GFM: column alignment;
- [ ] Limit useless string allocaations (via Cow / flexstr);
- [ ] Use rayon to improve speed;
- [ ] Custom formats via options (especially for xlsx/gfm);