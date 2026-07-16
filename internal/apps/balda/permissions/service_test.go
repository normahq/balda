package permissions

import (
	"context"
	"testing"
	"time"

	"github.com/baldaworks/go-actorlayer"
	actortransport "github.com/baldaworks/go-actorlayer/transport"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/permissioncmd"
	"github.com/normahq/balda/internal/apps/balda/questioncmd"
	"github.com/normahq/balda/internal/apps/balda/questions"
	baldastate "github.com/normahq/balda/internal/apps/balda/state"
	"github.com/rs/zerolog"
)

type reviewQuestionStore struct {
	record baldastate.QuestionRecord
}

func (s *reviewQuestionStore) CreatePendingQuestion(_ context.Context, record baldastate.QuestionRecord) error {
	s.record = record
	return nil
}
func (s *reviewQuestionStore) BindQuestionDeliveryRef(_ context.Context, questionID string, ref questioncmd.DeliveryRef) error {
	s.record.QuestionID = questionID
	s.record.Provider = ref.Provider
	s.record.ConversationKey = ref.ConversationKey
	s.record.ProviderMessageID = ref.ProviderMessageID
	return nil
}
func (s *reviewQuestionStore) GetQuestionByID(_ context.Context, questionID string) (baldastate.QuestionRecord, bool, error) {
	return s.record, s.record.QuestionID == questionID, nil
}
func (*reviewQuestionStore) GetPendingQuestionByReplyRef(context.Context, string, string, string) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, false, nil
}
func (*reviewQuestionStore) MarkQuestionAnswered(context.Context, string, questioncmd.Answer) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, false, nil
}
func (*reviewQuestionStore) MarkQuestionTimedOut(context.Context, string, time.Time) (baldastate.QuestionRecord, bool, error) {
	return baldastate.QuestionRecord{}, true, nil
}

type reviewDispatcher struct {
	envelopes chan actorlayer.Envelope
}

func (d reviewDispatcher) Dispatch(_ context.Context, envelope actorlayer.Envelope) (*actortransport.DispatchReceipt, error) {
	d.envelopes <- envelope
	return &actortransport.DispatchReceipt{}, nil
}

func TestParseConfigDefaults(t *testing.T) {
	config, err := ParseConfig("", "")
	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	if config.Mode != permissioncmd.ModeAllowAll || config.Timeout != 2*time.Minute {
		t.Fatalf("ParseConfig() = %+v", config)
	}
}

func TestReviewStaticPoliciesSelectByKind(t *testing.T) {
	options := []permissioncmd.Option{
		{ID: "reject", Kind: "reject_once"},
		{ID: "always", Kind: "allow_always"},
		{ID: "once", Kind: "allow_once"},
	}
	allow, err := New(Config{Mode: permissioncmd.ModeAllowAll, Timeout: time.Minute}, nil, nil, zerolog.Nop()).Review(context.Background(), permissioncmd.Request{Options: options})
	if err != nil {
		t.Fatalf("allow Review() error = %v", err)
	}
	if allow.OptionID != "once" {
		t.Fatalf("allow option = %q, want once", allow.OptionID)
	}
	deny, err := New(Config{Mode: permissioncmd.ModeDenyAll, Timeout: time.Minute}, nil, nil, zerolog.Nop()).Review(context.Background(), permissioncmd.Request{Options: options})
	if err != nil {
		t.Fatalf("deny Review() error = %v", err)
	}
	if deny.OptionID != "reject" {
		t.Fatalf("deny option = %q, want reject", deny.OptionID)
	}
}

func TestAskUnsupportedChannelFailsClosed(t *testing.T) {
	service := New(Config{Mode: permissioncmd.ModeAsk, Timeout: time.Minute}, nil, nil, zerolog.Nop())
	decision, err := service.Review(context.Background(), permissioncmd.Request{
		Interaction: questioncmd.InteractionContext{SessionID: "s1", ChannelKind: "zulip"},
		Options: []permissioncmd.Option{
			{ID: "allow", Kind: "allow_once"},
			{ID: "reject", Kind: "reject_once"},
		},
	})
	if err == nil {
		t.Fatal("Review() error = nil, want unsupported channel error")
	}
	if decision.OptionID != "reject" {
		t.Fatalf("decision = %+v, want reject", decision)
	}
}

func TestAskWaitsForGenericPermissionDecision(t *testing.T) {
	dispatcher := reviewDispatcher{envelopes: make(chan actorlayer.Envelope, 1)}
	service := New(
		Config{Mode: permissioncmd.ModeAsk, Timeout: time.Second},
		questions.New(&reviewQuestionStore{}, nil, zerolog.Nop()),
		dispatcher,
		zerolog.Nop(),
	)
	result := make(chan permissioncmd.Decision, 1)
	errors := make(chan error, 1)
	go func() {
		decision, err := service.Review(context.Background(), permissioncmd.Request{
			Interaction: questioncmd.InteractionContext{
				SessionID:   "tg-1-0",
				ChannelKind: "telegram",
				Locator: deliverycmd.Locator{
					ChannelType: "telegram",
					AddressKey:  "1:0",
					AddressJSON: `{"chat_id":1,"topic_id":0}`,
					SessionID:   "tg-1-0",
				},
				RequestedBy: questioncmd.UserRef{UserID: "tg-101"},
			},
			Options: []permissioncmd.Option{
				{ID: "allow", Name: "Allow once", Kind: "allow_once"},
				{ID: "reject", Name: "Reject once", Kind: "reject_once"},
			},
		})
		result <- decision
		errors <- err
	}()
	envelope := <-dispatcher.envelopes
	var delivery deliverycmd.Payload
	if err := actorlayer.UnmarshalPayload(envelope.Payload, &delivery); err != nil {
		t.Fatalf("decode delivery payload: %v", err)
	}
	if delivery.Question == nil || len(delivery.Question.Options) != 2 || delivery.Question.Options[0].ID != "allow" {
		t.Fatalf("delivery question = %+v", delivery.Question)
	}
	if delivery.Question.Audience.Visibility != deliverycmd.QuestionVisibilityPrivate || delivery.Question.Audience.UserID != "tg-101" {
		t.Fatalf("delivery audience = %+v", delivery.Question.Audience)
	}
	service.Resolve(delivery.Refs["question_id"], permissioncmd.Decision{OptionID: "allow", Source: "user"})
	if err := <-errors; err != nil {
		t.Fatalf("Review() error = %v", err)
	}
	if decision := <-result; decision.OptionID != "allow" || decision.Source != "user" {
		t.Fatalf("decision = %+v", decision)
	}
}
