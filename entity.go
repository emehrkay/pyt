package pyt

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type entity struct {
	ID          string `json:"id"`
	Active      bool   `json:"active"`
	Type        string `json:"type"`
	TimeCreated Time   `json:"time_created"`
	TimeUpdated Time   `json:"time_updated"`
}

type NodeSet[T any] []Node[T]

type Node[T any] struct {
	entity
	Properties T
}

type EdgeSet[T any] []Edge[T]

type Edge[T any] struct {
	entity
	InID       string `json:"in_id"`
	OutID      string `json:"out_id"`
	Properties T
}

type GenericProperties map[string]any

// Values converts the GenericProperties to be saved in the db
func (g GenericProperties) Value() (driver.Value, error) {
	if len(g) == 0 {
		return json.Marshal(GenericProperties{})
	}

	return json.Marshal(g)
}

// Scan does the work of translating the value stored in the database
// back into GenericProperties
func (g *GenericProperties) Scan(src any) error {
	if src == nil {
		return nil
	}

	source, ok := src.(string)
	if !ok {
		return fmt.Errorf(`not a stirng: %v`, src)
	}

	var a any
	err := json.Unmarshal([]byte(source), &a)
	if err != nil {
		return fmt.Errorf(`cannot unmarshal type: %v`, err)
	}

	*g, ok = a.(map[string]any)
	if !ok {
		return fmt.Errorf(`type is not a map[string]any -- %v`, a)
	}

	return nil
}

// PropertiesToType will take a byte of properties and
// unmarshal it into the provided type
func PropertiesToType[T any](properties []byte) (*T, error) {
	entity := new(T)
	err := json.Unmarshal([]byte(properties), entity)

	return entity, err
}

type TypedNodeEdgeSet[NodeType any, EdgeType any] []TypedNodeEdge[NodeType, EdgeType]

func (ty TypedNodeEdgeSet[NodeType, EdgeType]) Nodes() *NodeSet[NodeType] {
	set := make(NodeSet[NodeType], len(ty))

	for i, t := range ty {
		set[i] = *t.Node
	}

	return &set
}

func (ty TypedNodeEdgeSet[NodeType, EdgeType]) Edges() *EdgeSet[EdgeType] {
	set := make(EdgeSet[EdgeType], len(ty))

	for i, t := range ty {
		set[i] = *t.Edge
	}

	return &set
}

type TypedNodeEdge[NodeType any, EdgeType any] struct {
	Node *Node[NodeType]
	Edge *Edge[EdgeType]
}

type GenericNode Node[GenericProperties]
type GenericEdge Edge[GenericProperties]
type GenericEdgeNodeSet []GenericEdgeNode

func GenericEdgeNodeSetToTypes[NodeType any, EdgeType any](set GenericEdgeNodeSet) (*TypedNodeEdgeSet[NodeType, EdgeType], error) {
	res := TypedNodeEdgeSet[NodeType, EdgeType]{}

	for _, s := range set {
		node, err := GenericNodeToType[NodeType](s.GenericNode)
		if err != nil {
			return nil, err
		}

		edge, err := GenericEdgeToType[EdgeType](s.GenericEdge)
		if err != nil {
			return nil, err
		}

		res = append(res, TypedNodeEdge[NodeType, EdgeType]{
			Node: node,
			Edge: edge,
		})
	}

	return &res, nil
}

type GenericEdgeNode struct {
	GenericEdge
	GenericNode
}

// GenericEdgeToType will convert a GenericEdge to the provided typed Edge
func GenericEdgeToType[T any](edgeInstance GenericEdge) (*Edge[T], error) {
	by, err := json.Marshal(edgeInstance.Properties)
	if err != nil {
		return nil, err
	}

	ty, err := PropertiesToType[T](by)
	if err != nil {
		return nil, err
	}

	ne := NewEdge[T](edgeInstance.ID, edgeInstance.Type, edgeInstance.InID, edgeInstance.OutID, *ty)

	return ne, nil
}

// GenericNodeToType will convert a GenericNode to the provided typed Node
func GenericNodeToType[T any](nodeInstance GenericNode) (*Node[T], error) {
	by, err := json.Marshal(nodeInstance.Properties)
	if err != nil {
		return nil, err
	}

	ty := new(T)
	err = json.Unmarshal(by, ty)
	if err != nil {
		return nil, err
	}

	ne := NewNode[T](nodeInstance.ID, nodeInstance.Type, *ty)

	return ne, nil
}

// NewNode creates a typed Node
func NewNode[T any](id, nodeType string, properties T) *Node[T] {
	return &Node[T]{
		entity: entity{
			ID:     id,
			Type:   nodeType,
			Active: true,
		},
		Properties: properties,
	}
}

// NewEdge creates a typed Edge
func NewEdge[T any](id, edgeType, inID, outID string, properties T) *Edge[T] {
	return &Edge[T]{
		entity: entity{
			ID:     id,
			Type:   edgeType,
			Active: true,
		},
		InID:       inID,
		OutID:      outID,
		Properties: properties,
	}
}

// Custom time taken from https://www.golang.dk/articles/go-and-sqlite-in-the-cloud
type Time struct {
	T time.Time
}

// rfc3339Milli is like time.RFC3339Nano, but with millisecond precision, and fractional seconds do not have trailing
// zeros removed.
const rfc3339Milli = "2006-01-02T15:04:05.000Z07:00"

// Value satisfies driver.Valuer interface.
func (t *Time) Value() (driver.Value, error) {
	return t.T.UTC().Format(rfc3339Milli), nil
}

// Scan satisfies sql.Scanner interface.
func (t *Time) Scan(src any) error {
	if src == nil {
		return nil
	}

	s, ok := src.(string)
	if !ok {
		return fmt.Errorf("error scanning time, got %+v", src)
	}

	parsedT, err := time.Parse(rfc3339Milli, s)
	if err != nil {
		return err
	}

	t.T = parsedT.UTC()

	return nil
}
