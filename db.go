package pyt

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// BuildSchema does the work of scaffoling the database and
// should be called when the connection is created.
func BuildSchema(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS node (
			id TEXT NOT NULL UNIQUE PRIMARY KEY,
			active BOOLEAN,
			type TEXT NOT NULL,
			properties TEXT,
			time_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			time_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,

		`CREATE TRIGGER IF NOT EXISTS node_time_created_trigger
		AFTER INSERT ON node
		BEGIN
			UPDATE
				node 
			SET 
				time_created = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
			WHERE id = NEW.id;
		END;`,

		`CREATE TRIGGER IF NOT EXISTS node_time_updated_trigger
		AFTER UPDATE ON node
		BEGIN
			UPDATE
				node 
			SET 
				time_updated = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
			WHERE id = NEW.id;
		END;`,

		`CREATE INDEX IF NOT EXISTS id_idx ON node(id);`,

		`CREATE INDEX IF NOT EXISTS type_idx ON node(type);`,

		`CREATE INDEX IF NOT EXISTS time_created_idx ON node(time_created);`,

		`CREATE INDEX IF NOT EXISTS time_updated_idx ON node(time_updated);`,

		`CREATE TABLE IF NOT EXISTS edge (
			id TEXT NOT NULL UNIQUE PRIMARY KEY,
			active BOOLEAN,
			type TEXT NOT NULL,
			in_id TEXT,
			out_id TEXT,
			properties TEXT,
			time_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			time_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(in_id, out_id, properties) ON CONFLICT REPLACE,
			FOREIGN KEY(in_id) REFERENCES node(id),
			FOREIGN KEY(out_id) REFERENCES node(id)
		);`,

		`CREATE INDEX IF NOT EXISTS in_id_idx ON edge(in_id);`,

		`CREATE INDEX IF NOT EXISTS out_id_idx ON edge(out_id);`,

		`CREATE INDEX IF NOT EXISTS type_idx ON edge(type);`,

		`CREATE INDEX IF NOT EXISTS time_created_idx ON edge(time_created);`,

		`CREATE INDEX IF NOT EXISTS time_updated_idx ON edge(time_updated);`,

		`CREATE TRIGGER IF NOT EXISTS edge_time_created_trigger
		AFTER INSERT ON edge
		BEGIN
			UPDATE
				edge 
			SET 
				time_created = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
			WHERE id = NEW.id;
		END;`,

		`CREATE TRIGGER IF NOT EXISTS edge_time_updated_trigger
		AFTER UPDATE ON edge
		BEGIN
			UPDATE
				edge 
			SET 
				time_updated = STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')
			WHERE id = NEW.id;
		END;`,
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, query := range queries {
		_, err := tx.Exec(query)
		if err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}

	return tx.Commit()
}

// ResultToNode is a utility function that will convert an sql.Row into
// a typed Node
func ResultToNode[T any](row *sql.Row, tx *sql.Tx) (*Node[T], error) {
	entity := new(Node[T])
	var newProperties string

	err := row.Scan(&entity.entity.ID, &entity.entity.Active, &entity.entity.Type, &newProperties, &entity.entity.TimeCreated, &entity.entity.TimeUpdated)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	newProps, err := PropertiesToType[T]([]byte(newProperties))
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	entity.Properties = *newProps

	return entity, nil
}

// RowsToNode is a utility method that is used to convert an sql.Rows instance
// into a typed NodeSet
func RowsToNode[T any](rows *sql.Rows, tx *sql.Tx) (*NodeSet[T], error) {
	var nodes NodeSet[T]

	for rows.Next() {
		newNode := new(Node[T])
		var properties string
		err := rows.Scan(&newNode.entity.ID, &newNode.entity.Active, &newNode.entity.Type, &properties, &newNode.entity.TimeCreated, &newNode.entity.TimeUpdated)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		props, err := PropertiesToType[T]([]byte(properties))
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		newNode.Properties = *props
		nodes = append(nodes, *newNode)
	}

	return &nodes, nil
}

// RowsToEdge is a utility method that is used to convert an sql.Rows instance
// into a typed EdgeSet
func RowsToEdge[T any](rows *sql.Rows, tx *sql.Tx) (*EdgeSet[T], error) {
	var nodes EdgeSet[T]

	for rows.Next() {
		newEdge := new(Edge[T])
		var properties string
		err := rows.Scan(&newEdge.entity.ID, &newEdge.entity.Active, &newEdge.entity.Type, &newEdge.InID, &newEdge.OutID, &properties, &newEdge.entity.TimeCreated, &newEdge.entity.TimeUpdated)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		props, err := PropertiesToType[T]([]byte(properties))
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		newEdge.Properties = *props
		nodes = append(nodes, *newEdge)
	}

	return &nodes, nil
}

// NodeCreate will add a node to the database
func NodeCreate[T any](tx *sql.Tx, newNode Node[T]) (*Node[T], error) {
	nodes, err := NodesCreate(tx, newNode)
	if err != nil {
		return nil, err
	}

	if nodes == nil || len(*nodes) == 0 {
		return nil, sql.ErrNoRows
	}

	return &(*nodes)[0], nil
}

// NodesCreate will add mulitple nodes to the database
func NodesCreate[T any](tx *sql.Tx, newNodes ...Node[T]) (*NodeSet[T], error) {
	var err error

	values := make([]string, len(newNodes))
	params := []any{}

	for i := 0; i < len(newNodes); i++ {
		values[i] = "(?, ?, ?, ?)"
		properties, err := json.Marshal(newNodes[i].Properties)
		if err != nil {
			return nil, err
		}
		params = append(params, newNodes[i].entity.ID, newNodes[i].entity.Active, newNodes[i].entity.Type, string(properties))
	}

	query := fmt.Sprintf(`
	INSERT INTO
		node
		(id, active, type, properties)
	VALUES
		%s
	RETURNING
		*
	`, strings.Join(values, ","))

	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	res, err := stmt.Query(params...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())
	}

	defer res.Close()

	nodes, err := RowsToNode[T](res, tx)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	return nodes, nil
}

// NodeUpdate updates a node's properties. updatedNode.ID must exist in the database
func NodeUpdate[T any](tx *sql.Tx, updatedNode Node[T], withReturn bool) (*Node[T], error) {
	var err error

	query := `
	UPDATE
		node
	SET
		active = ?,
		properties = ?
	WHERE
		id = ?
	RETURNING
		*
	`
	properties, err := json.Marshal(updatedNode.Properties)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(query, updatedNode.entity.Active, string(properties), updatedNode.ID)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	if !withReturn {
		return nil, nil
	}

	node, err := NodeGetByID[T](tx, updatedNode.ID)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	return node, nil
}

// NodeGetByID retrieves and typed node by its id
func NodeGetByID[T any](tx *sql.Tx, id string) (*Node[T], error) {
	fil := FilterSet{
		NewFilter("id", id),
	}

	return NodeGetBy[T](tx, fil)
}

// NodeGetBy retuns a single typed node by filters
func NodeGetBy[T any](tx *sql.Tx, filters FilterSet) (*Node[T], error) {
	nodes, err := NodesGetBy[T](tx, &filters)
	if err != nil {
		return nil, err
	}

	if nodes == nil || len(*nodes) == 0 {
		return nil, sql.ErrNoRows
	}

	return &(*nodes)[0], nil
}

// NodesGetBy will return a typed NodeSet and can be extended using a FilterSet
func NodesGetBy[T any](tx *sql.Tx, filters *FilterSet) (*NodeSet[T], error) {
	params := []any{}
	var where string
	var err error

	if filters != nil {
		clasuses := filters.Build(&params)
		if clasuses != "" {
			where = fmt.Sprintf(`WHERE
			%s`, clasuses)
		}
	}

	query := fmt.Sprintf(`
	SELECT
		*
	FROM
		node
	%s
	`, where)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Query(params...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}
	defer res.Close()

	nodes, err := RowsToNode[T](res, tx)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	return nodes, nil
}

// NodesOutRelatedBy will do a single out hop from nodeID via the edgeType
// can be extended with a FilterSet the edge table is aliased as e, and the
// node table is aliased as n
func NodesOutRelatedBy(tx *sql.Tx, nodeID, edgeType string, filters *FilterSet) (*GenericEdgeNodeSet, error) {
	return NodesGetRelatedBy(tx, nodeID, "out", edgeType, filters)
}

// NodesInRelatedBy will do a single in hop from nodeID via the edgeType
// can be extended with a FilterSet the edge table is aliased as e, and the
// node table is aliased as n
func NodesInRelatedBy(tx *sql.Tx, nodeID, edgeType string, filters *FilterSet) (*GenericEdgeNodeSet, error) {
	return NodesGetRelatedBy(tx, nodeID, "in", edgeType, filters)
}

// NodesGetRelatedBy will do a single in or out hop from nodeID via the edgeType
// can be extended with a FilterSet the edge table is aliased as e, and the
// node table is aliased as n
func NodesGetRelatedBy(tx *sql.Tx, nodeID, direction, edgeType string, filters *FilterSet) (*GenericEdgeNodeSet, error) {
	var err error

	edgeWhere := "in_id"
	edgeJoin := "out_id"

	if direction == "in" {
		edgeJoin = "in_id"
		edgeWhere = "out_id"
	}

	params := []any{nodeID, edgeType}

	query := fmt.Sprintf(`
	SELECT
		e.id as edge_id,
		e.type as edge_type,
		e.in_id as edge_in_id,
		e.out_id as edge_out_id,
		e.properties as edge_properties,
		e.time_created as edge_time_created,
		e.time_updated as edge_time_updated,
		n.id as node_id,
		n.type as node_type,
		n.properties as node_properties,
		n.time_created as node_time_created,
		n.time_updated as node_time_updated
	FROM
		edge e
	JOIN
		node n ON n.id = e.%s
	WHERE
		e.%s = ?
	AND
		e.type = ?
	`, edgeJoin, edgeWhere)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	rows, err := stmt.Query(params...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	defer rows.Close()

	var resp GenericEdgeNodeSet

	for rows.Next() {
		rec := GenericEdgeNode{}
		err := rows.Scan(
			&rec.GenericEdge.entity.ID,
			&rec.GenericEdge.entity.Type,
			&rec.GenericEdge.InID,
			&rec.GenericEdge.OutID,
			&rec.GenericEdge.Properties,
			&rec.GenericEdge.entity.TimeCreated,
			&rec.GenericEdge.entity.TimeUpdated,
			&rec.GenericNode.entity.ID,
			&rec.GenericNode.entity.Type,
			&rec.GenericNode.Properties,
			&rec.GenericNode.entity.TimeCreated,
			&rec.GenericNode.entity.TimeUpdated,
		)
		if err != nil {
			return nil, err
		}

		resp = append(resp, rec)
	}

	return &resp, nil
}

// EdgeCreate will add an edge to the database. The InID and OutID nodes
// must already exist in the database or are apart of the current transaction
func EdgeCreate[T any](tx *sql.Tx, newEdge Edge[T]) (*Edge[T], error) {
	edges, err := EdgesCreate[T](tx, newEdge)
	if err != nil {
		return nil, err
	}

	if edges == nil || len(*edges) == 0 {
		return nil, sql.ErrNoRows
	}

	return &(*edges)[0], nil
}

// EdgesCreate will add mulitple edges to the database. The InID and OutID nodes
// for each edge must already exist in the database or are apart of the current transaction
func EdgesCreate[T any](tx *sql.Tx, newEdges ...Edge[T]) (*EdgeSet[T], error) {
	var err error

	values := make([]string, len(newEdges))
	params := []any{}

	for i := 0; i < len(newEdges); i++ {
		values[i] = "(?, ?, ?, ?, ?, ?)"
		properties, err := json.Marshal(newEdges[i].Properties)
		if err != nil {
			return nil, err
		}

		params = append(params, newEdges[i].entity.ID, newEdges[i].entity.Active, newEdges[i].entity.Type, newEdges[i].InID, newEdges[i].OutID, string(properties))
	}

	query := fmt.Sprintf(`
	INSERT INTO
		edge
		(id, active, type, in_id, out_id, properties)
	VALUES
		%s
	RETURNING
		*
	`, strings.Join(values, ","))
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Query(params...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	defer res.Close()

	edges, err := RowsToEdge[T](res, tx)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	return edges, nil
}

// EdgeUpdate will update the properties on an existing edge
func EdgeUpdate[T any](tx *sql.Tx, updatedEdge Edge[T], withReturn bool) (*Edge[T], error) {
	var err error

	query := `
	UPDATE
		edge
	SET
		active = ?,
		properties = ?
	WHERE
		id = ?
	RETURNING
		*
	`
	properties, err := json.Marshal(updatedEdge.Properties)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(query, updatedEdge.entity.Active, string(properties), updatedEdge.ID)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	if !withReturn {
		return nil, nil
	}

	edge, err := EdgeGetByID[T](tx, updatedEdge.ID)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	return edge, nil
}

// EdgeGetByID will return a typed edge by its id
func EdgeGetByID[T any](tx *sql.Tx, id string) (*Edge[T], error) {
	fil := FilterSet{
		NewFilter("id", id),
	}

	return EdgeGetBy[T](tx, fil)
}

// EdgeGetByID will return a single typed edge by its id
func EdgeGetBy[T any](tx *sql.Tx, filters FilterSet) (*Edge[T], error) {
	edges, err := EdgesGetBy[T](tx, &filters)
	if err != nil {
		return nil, err
	}

	if edges == nil || len(*edges) == 0 {
		return nil, sql.ErrNoRows
	}

	return &(*edges)[0], nil
}

// EdgesGetBy will return a typed EdgeSet and can be extended using a FilterSet
func EdgesGetBy[T any](tx *sql.Tx, filters *FilterSet) (*EdgeSet[T], error) {
	params := []any{}
	var where string
	var err error

	if filters != nil {
		clasuses := filters.Build(&params)
		if clasuses != "" {
			where = fmt.Sprintf(`WHERE
			%s`, clasuses)
		}
	}

	query := fmt.Sprintf(`
	SELECT
		*
	FROM
		edge
	%s
	`, where)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	res, err := stmt.Query(params...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}
	defer res.Close()

	edges, err := RowsToEdge[T](res, tx)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback())

	}

	return edges, nil
}
