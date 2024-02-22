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
    - `active` <bool> -- easy way to soft delete
    - `properties` <text> indexed -- a json string of the key => val pairs for the entity
    - `time_created` and `time_updated` <timestamp> indexed -- automatically updated when its respective action is taken on the record
    - All database columns are explicit, no virtual columns whose values are derived from the properties
1. While entities (`Node[T]` `Edge[T]`) can be manually created, it is easier to use the constructor functions (`NewNode` `NewEdge`). The only reason they arent private is to allow for extendability
1. Create your own sqlite instance. Just make sure that you add `?_foreign_keys=true` when creating it.

> Tests are coming, I wanted to make sure that I liked the api before writing them

## Quickstart

I'm going to show you how to build Twitter using P.Y.T. (see [twitter in examples](/examples/twitter/main.go))


> all error handling is omitted

1. Set up your project

```
mkdir twitter
cd twitter
go mod init github.com/username/twitter
```

2. Install P.Y.T.

```
go get github.com/emehrkay/pyt
go mod tidy
```

3. Connect to sqlite and build the schema
```go
db, err := sql.Open("sqlite3", "./twitter.db?_foreign_keys=true")
err = pyt.BuildSchema(db)
```

4. Given this basic schema, we'll define some types for nodes and edges (the json tag will be the property name in the database) 

```
  (follows)
   |     |
   |     |
   |     |
   V     |
+--------+----+                     +------------+
|             |                     |            |
|             |                     |            |
|    user     +------(wrote)------->|   tweet    |
|             |                     |            |
|             |                     |            |
+-------------+                     +------------+               
```

```go
// nodes
type User struct {
    Username string `json:"username"`
}

type Tweet struct {
    Body string `json:"body"`
}

// edges
type Follows struct {}

type Wrote struct {}
```

5. Add some users

```go
mark := pyt.NewNode(uuid.NewString(), "user", User{
    Username: "mark",
})
kram := pyt.NewNode(uuid.NewString(), "user", User{
    Username: "kram",
})
you := pyt.NewNode(uuid.NewString(), "user", User{
    Username: "you",
})
users, err := pyt.NodesCreate(tx, *mark, *kram, *you)
```

6. Create some follower connections

```go
mk := pyt.NewEdge(uuid.NewString(), "follows", mark.ID, kram.ID, Follows{})
km := pyt.NewEdge(uuid.NewString(), "follows", kram.ID, mark.ID, Follows{})
yk := pyt.NewEdge(uuid.NewString(), "follows", you.ID, kram.ID, Follows{})
ym := pyt.NewEdge(uuid.NewString(), "follows", you.ID, mark.ID, Follows{})
_, err = pyt.EdgesCreate(tx, *mk, *km, *yk, *ym)
```

7. Add some tweets for all of the users and add a `wrote` edge between the user and the tweet

```go
for x, user := range *users {
    total := 50

    if x == 1 {
        total = 20
    } else if x == 2 {
        total = 10
    }

    for i := 0; i < total; i++ {
        mt := pyt.NewNode(uuid.NewString(), "tweet", Tweet{
            Body: fmt.Sprintf("%s tweeted item #%v", user.Properties.Username, i),
        })
        _, err := pyt.NodeCreate(tx, *mt)

        // arbitary sleep
        time.Sleep(time.Millisecond * 1)
        wrote := pyt.NewEdge(uuid.NewString(), "wrote", user.ID, mt.ID, Wrote{})
        _, err = pyt.EdgeCreate(tx, *wrote)
    }
}
```

8. Now that we have the tables seeded with some data, lets pull it out. We can accomplish this by selecting from the edge table and joining on the node and edge tables as a way to walk the graph

```sql
SELECT
	json_extract(follows.properties, '$.username') as author,
	follows.id as author_id,
	tweet.id as tweet_id,
	json_extract(tweet.properties, '$.body') as tweet,
	tweet.time_created as date
FROM
	edge e
JOIN
	node follows ON follows.id = e.out_id
JOIN
	edge wrote ON wrote.in_id = follows.id
JOIN
	node tweet ON tweet.id = wrote.out_id
WHERE
	e.in_id = '10a9a97d-2a07-441f-bfcb-70177fcc25c7'
AND
	e.type = 'follows'
AND
	wrote.type = 'wrote'
ORDER BY
	tweet.time_created DESC
```

There is a lot going on here, but it isnt too bad. First we're starting with our user's (`10a9a97d-2a07-441f-bfcb-70177fcc25c7`) edges. We limit the edges based on `follows` type. We then join aginst node, alised as `follows` on it's id and the edge's out_id. Join on edge, alias as `wrote` and we limit those in the where clause `wrote.type = 'wrote'` and finally we get the tweet by joing wrote edge on the node table again. Finally we order the results by the time it was created

```go
type FollowersTweet struct {
	author    string
	author_id string
	tweet_id  string
	tweet     string
	date      time.Time
}

type FollowersTweets []FollowersTweet

func (ft FollowersTweets) WriteTable() {
	tw := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	fmt.Fprintln(tw, "author\ttweet\ttime")

	for _, f := range ft {
		row := fmt.Sprintf("%v\t%v\t%v", f.author, f.tweet, f.date)
		fmt.Fprintln(tw, row)
	}

	fmt.Printf("found %d tweets\n\n", len(ft))
	tw.Flush()
	fmt.Println("\n ")
}

func getFollingTweets(tx *sql.Tx, userID string) (*FollowersTweets, error) {
	query := `
	SELECT
		json_extract(follows.properties, '$.username') as author,
		follows.id as author_id,
		tweet.id as tweet_id,
		json_extract(tweet.properties, '$.body') as tweet,
		tweet.time_created as date
	FROM
		edge e
	JOIN
		node follows ON follows.id = e.out_id
	JOIN
		edge wrote ON wrote.in_id = follows.id
	JOIN
		node tweet ON tweet.id = wrote.out_id
	WHERE
		e.in_id = ?
	AND
		wrote.type = 'wrote'
	ORDER BY
		wrote.time_created DESC
	`
	rows, err := tx.Query(query, userID)

	var resp FollowersTweets

	for rows.Next() {
		rec := FollowersTweet{}
		err := rows.Scan(
			&rec.author,
			&rec.author_id,
			&rec.tweet_id,
			&rec.tweet,
			&rec.date,
		)
		if err != nil {
			return nil, err
		}

		resp = append(resp, rec)
	}

	return &resp, nil
}
```

9. Get a timeline of tweets from the users that `you` is following

```go
timeline, err := getFollingTweets(tx, you.ID)
timeline.WriteTable()
```

```
found 70 tweets

author tweet                 time
kram   kram tweeted item #19 2024-02-22 17:03:59.693 +0000 UTC
kram   kram tweeted item #18 2024-02-22 17:03:59.691 +0000 UTC
kram   kram tweeted item #17 2024-02-22 17:03:59.69 +0000 UTC
kram   kram tweeted item #16 2024-02-22 17:03:59.689 +0000 UTC
kram   kram tweeted item #15 2024-02-22 17:03:59.688 +0000 UTC
...
mark   mark tweeted item #4  2024-02-22 17:03:59.6 +0000 UTC
mark   mark tweeted item #3  2024-02-22 17:03:59.598 +0000 UTC
mark   mark tweeted item #2  2024-02-22 17:03:59.596 +0000 UTC
mark   mark tweeted item #1  2024-02-22 17:03:59.594 +0000 UTC
mark   mark tweeted item #0  2024-02-22 17:03:59.592 +0000 UTC
```
