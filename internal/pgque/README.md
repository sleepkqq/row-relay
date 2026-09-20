# Bundled PgQue

Unmodified PgQue **0.2.0**, commit
`e8ee488d2c1d87ab09eed2581ec4bbc1e68315f6` from
<https://github.com/NikolayS/PgQue>. `pgque.sql`, `LICENSE` and `NOTICE` are
vendored verbatim. A unit check verifies the SQL SHA-256.

`row-relay --install-pgque` executes this SQL in a transaction using the supplied
administrative database connection. It refuses an existing `pgque` schema;
it is not an upgrade command. Normal relay startup never installs anything.
