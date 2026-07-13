package deliveryfx

import (
	"time"

	baldachannel "github.com/normahq/balda/internal/apps/balda/channel"
	baldaslack "github.com/normahq/balda/internal/apps/balda/channel/slack"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldazulip "github.com/normahq/balda/internal/apps/balda/channel/zulip"
	"github.com/normahq/balda/internal/apps/balda/deliverycmd"
	"github.com/normahq/balda/internal/apps/balda/messenger"
	"github.com/rs/zerolog"
	"github.com/tgbotkit/client"
	"go.uber.org/fx"
)

var Module = fx.Module("balda_deliveryfx",
	fx.Provide(
		fx.Annotate(
			func(
				tgClient client.ClientWithResponsesInterface,
				logger zerolog.Logger,
				formattingMode string,
			) *messenger.Messenger {
				m := messenger.NewMessenger(tgClient, logger)
				m.SetAgentReplyFormattingMode(formattingMode)
				return m
			},
			fx.ParamTags(``, ``, `name:"balda_telegram_formatting_mode"`),
		),
		fx.Annotate(
			func(m *messenger.Messenger) baldatelegram.TelegramMessenger { return m },
		),
		func(params baldatelegram.AdapterParams) *baldatelegram.Adapter {
			adapter := baldatelegram.NewAdapter(params)
			adapter.SetTypingThrottleInterval(4 * time.Second)
			return adapter
		},
		func(client *baldazulip.Client, logger zerolog.Logger) *baldazulip.Adapter {
			adapter := baldazulip.NewAdapter(client, logger)
			adapter.SetTypingThrottleInterval(4 * time.Second)
			return adapter
		},
		baldaslack.NewAdapter,
		func(tg *baldatelegram.Adapter, zu *baldazulip.Adapter, sl *baldaslack.Adapter) *baldachannel.Router {
			return baldachannel.NewRouter(map[string]deliverycmd.Adapter{
				string(deliverycmd.ChannelTypeTelegram): tg,
				string(deliverycmd.ChannelTypeZulip):    zu,
				string(deliverycmd.ChannelTypeSlack):    sl,
			})
		},
	),
)
