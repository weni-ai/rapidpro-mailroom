package dynsvc

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nyaruka/gocommon/aws/dynamo"
)

// Service is the gocommon v1.60.5 Dynamo wrapper. gocommon v1.75.7 replaced it with
// Client/Writer/Item (PK/SK); Mumbai still writes ChannelLogs keyed by UUID.
type Service struct {
	Client      *dynamodb.Client
	tablePrefix string
}

func NewService(accessKey, secretKey, region, endpoint, tablePrefix string) (*Service, error) {
	client, err := dynamo.NewClient(accessKey, secretKey, region, endpoint)
	if err != nil {
		return nil, err
	}
	return &Service{Client: client, tablePrefix: tablePrefix}, nil
}

func (s *Service) Test(ctx context.Context) error {
	_, err := s.Client.ListTables(ctx, &dynamodb.ListTablesInput{})
	return err
}

func (s *Service) TableName(base string) string {
	return s.tablePrefix + base
}

func (s *Service) GetItem(ctx context.Context, table string, key map[string]types.AttributeValue, dst any) error {
	resp, err := s.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.TableName(table)),
		Key:       key,
	})
	if err != nil {
		return fmt.Errorf("error getting item from dynamo: %w", err)
	}
	if err := unmarshal(resp.Item, dst); err != nil {
		return fmt.Errorf("error unmarshaling dynamo item: %w", err)
	}
	return nil
}

func (s *Service) PutItem(ctx context.Context, table string, v any) error {
	item, err := marshal(v)
	if err != nil {
		return fmt.Errorf("error marshaling dynamo item: %w", err)
	}
	_, err = s.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.TableName(table)),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("error putting item to dynamo: %w", err)
	}
	return nil
}

type marshaler interface {
	MarshalDynamo() (map[string]types.AttributeValue, error)
}

type unmarshaler interface {
	UnmarshalDynamo(map[string]types.AttributeValue) error
}

func marshal(v any) (map[string]types.AttributeValue, error) {
	if m, ok := v.(marshaler); ok {
		return m.MarshalDynamo()
	}
	return attributevalue.MarshalMap(v)
}

func unmarshal(m map[string]types.AttributeValue, v any) error {
	if u, ok := v.(unmarshaler); ok {
		return u.UnmarshalDynamo(m)
	}
	return attributevalue.UnmarshalMap(m, v)
}
