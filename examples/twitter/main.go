package main

import (
	"database/sql"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	"github.com/emehrkay/pyt"
)

func init() {
	pyt.NodeTableName = "node_xxx_yyy"
	pyt.EdgeTableName = "anything_but_the_e_word"
}

// nodes
type User struct {
	Username string `json:"username"`
}

type Tweet struct {
	Body string `json:"body"`
}

// edges
type Follows struct{}

type Wrote struct{}

func main() {
	// use this if you want to save to an actual file
	path := "twitter.db"
	os.Remove(path)

	path = path + "?_foreign_keys=true"
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		p(`cannot create db`, err)
	}

	tx, err := db.Begin()
	if err != nil {
		p(`cannot start transaction`, err)
	}

	defer tx.Commit()

	err = pyt.BuildSchema(db)
	if err != nil {
		p(`cannot build tables`, err)
	}

	// let's make the username unique for user types
	query := fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS user_username_idx ON %s(type, json_extract(properties, '$.username')) WHERE type = 'user'`, pyt.NodeTableName)
	_, err = db.Exec(query)
	if err != nil {
		p(`unable to add unique user constraint`, err)
	}
	tx.Commit()
	tx, _ = db.Begin()

	// add some users
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
	if err != nil {
		p(`cannot create users`, err)
	}
	tx.Commit()
	tx, _ = db.Begin()

	// followers
	mk := pyt.NewEdge(uuid.NewString(), "follows", mark.ID, kram.ID, Follows{})
	km := pyt.NewEdge(uuid.NewString(), "follows", kram.ID, mark.ID, Follows{})
	yk := pyt.NewEdge(uuid.NewString(), "follows", you.ID, kram.ID, Follows{})
	ym := pyt.NewEdge(uuid.NewString(), "follows", you.ID, mark.ID, Follows{})
	_, err = pyt.EdgesCreate(tx, *mk, *km, *yk, *ym)
	if err != nil {
		p(`cannot save followers`, err)
	}
	tx.Commit()
	tx, _ = db.Begin()

	// add some tweets
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
			if err != nil {
				p(`unable to create tweet`, err)
			}

			// arbitary sleep
			time.Sleep(time.Millisecond * 1)
			wrote := pyt.NewEdge(uuid.NewString(), "wrote", user.ID, mt.ID, Wrote{})
			_, err = pyt.EdgeCreate(tx, *wrote)
			if err != nil {
				p(`unable to connect tweet to user`, err)
			}
		}
	}
	tx.Commit()
	tx, _ = db.Begin()

	// get some data
	timeline, err := getFollingTweets(tx, you.ID)
	if err != nil {
		p("cant load timeline", err)
	}
	tx.Commit()
	timeline.WriteTable()
}

type FollowersTweet struct {
	author    string
	author_id string
	tweet_id  string
	tweet     string
	date      pyt.Time
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
	query := fmt.Sprintf(`
	SELECT
		json_extract(follows.properties, '$.username') as author,
		follows.id as author_id,
		tweet.id as tweet_id,
		json_extract(tweet.properties, '$.body') as tweet,
		tweet.time_created as date
	FROM
		%[1]s e
	JOIN
		%[2]s follows ON follows.id = e.out_id
	JOIN
		%[1]s wrote ON wrote.in_id = follows.id
	JOIN
		%[2]s tweet ON tweet.id = wrote.out_id
	WHERE
		e.in_id = ?
	AND
		e.type = 'follows'
	AND
		wrote.type = 'wrote'
	ORDER BY
		tweet.time_created DESC
	`, pyt.EdgeTableName, pyt.NodeTableName)
	rows, err := tx.Query(query, userID)
	if err != nil {
		p(`unable to get followers' tweets`, err)
	}

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

func p(msg string, err error) {
	panic(fmt.Sprintf(`%v -- %v`, msg, err))
}
