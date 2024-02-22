# P.Y.T.

> pretty young thing

An opinionated Go SQLite graph database based on [simple-graph](https://github.com/dpapathanasiou/simple-graph).

## Opinions

1. All data is typed
    - There is a way to use a `map[string]any` for properties if you really wanted to
1. All querying is done via a transaction
1. Both the `node` and `edge` tables have common columns:
    - `id` <string> -- unique and must be explictly defined and unique to the table. I've been using a `uuid` but any unique string should work
    - `type` <text> indexed -- the type of entity that is stored. This is a easy way to classify and segement data
    - `properties` <text> indexed -- a json string of the key => val pairs for the entity
    - `time_created` and `time_updated` <timestamp> indexed -- automatically updated when its respective action is taken on the record
    - All database columns are explicit, no virtual columns that extra value from the properties
1. While entities (`Node[T]` `Edge[T]`) can be manually created, it is easier to use the constructor functions (`NewNode` `NewEdge`). The only reason they arent private is to allow for extendability

## Example

I'm going to show you how to build Twitter using P.Y.T.

