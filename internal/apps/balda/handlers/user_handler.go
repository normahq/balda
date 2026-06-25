package handlers

import (
	"context"
	"fmt"
	"strings"

	"github.com/normahq/balda/internal/apps/balda/auth"
	baldatelegram "github.com/normahq/balda/internal/apps/balda/channel/telegram"
	baldasession "github.com/normahq/balda/internal/apps/balda/session"
	actortransport "github.com/normahq/balda/pkg/actorlayer/transport"
	"github.com/tgbotkit/client"
	"go.uber.org/fx"
)

type userHandler struct {
	ownerStore        *auth.OwnerStore
	inviteStore       *auth.InviteStore
	collaboratorStore *auth.CollaboratorStore
	channel           *baldatelegram.Adapter
	actorDispatcher   actortransport.Dispatcher
	tgClient          client.ClientWithResponsesInterface
	botUsername       string
}

type userHandlerParams struct {
	fx.In

	OwnerStore        *auth.OwnerStore
	InviteStore       *auth.InviteStore
	CollaboratorStore *auth.CollaboratorStore
	Channel           *baldatelegram.Adapter
	ActorDispatcher   actortransport.Dispatcher
	TGClient          client.ClientWithResponsesInterface `optional:"true"`
}

func (h *userHandler) getBotUsername(ctx context.Context) string {
	if h.botUsername != "" {
		return h.botUsername
	}
	if h.tgClient == nil {
		return ""
	}
	resp, err := h.tgClient.GetMeWithResponse(ctx)
	if err != nil {
		return ""
	}
	if resp.JSON200 == nil || resp.JSON200.Result.Username == nil {
		return ""
	}
	h.botUsername = *resp.JSON200.Result.Username
	return h.botUsername
}

func (h *userHandler) HandleUserCommand(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	if !h.ownerStore.IsOwner(commandCtx.UserID) {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "This command is only for the owner."); err != nil {
			return err
		}
		return nil
	}

	args := strings.Fields(commandCtx.Args)
	if len(args) == 0 {
		return h.sendUsage(ctx, commandCtx.Locator)
	}

	switch args[0] {
	case userActionAdd:
		return h.onAdd(ctx, commandCtx)
	case userActionList:
		return h.onList(ctx, commandCtx)
	case userActionRemove:
		return h.onRemove(ctx, commandCtx)
	default:
		return h.sendUsage(ctx, commandCtx.Locator)
	}
}

func (h *userHandler) sendUsage(ctx context.Context, locator baldasession.SessionLocator) error {
	usage := "Usage:\n" +
		"• /user add - Generate invite link\n" +
		"• /user list - Show collaborators and active invites\n" +
		"• /user remove <user_id> - Remove collaborator by ID\n"
	return sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, locator, usage)
}

func (h *userHandler) onAdd(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	ownerID := fmt.Sprintf("%d", commandCtx.UserID)

	token, _, err := h.inviteStore.CreateInvite(ctx, ownerID)
	if err != nil {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "Failed to create invite. Please try again."); err != nil {
			return err
		}
		return nil
	}

	username := strings.TrimSpace(h.getBotUsername(ctx))
	if username == "" {
		username = "<bot_username>"
	}
	inviteLink := fmt.Sprintf("https://t.me/%s?start=invite_%s", username, token)
	message := fmt.Sprintf("Invite link created:\n%s\n\nVisit this link to become a bot collaborator", inviteLink)

	if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, message); err != nil {
		return err
	}
	return nil
}

func (h *userHandler) onList(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	var lines []string

	collaborators, err := h.collaboratorStore.ListCollaborators(ctx)
	if err != nil {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "Failed to list collaborators. Please try again."); err != nil {
			return err
		}
		return nil
	}

	if len(collaborators) > 0 {
		lines = append(lines, "Collaborators:")
		for _, c := range collaborators {
			name := "unknown"
			if strings.TrimSpace(c.Username) != "" {
				name = "@" + c.Username
			} else if strings.TrimSpace(c.FirstName) != "" {
				name = c.FirstName
			}
			lines = append(lines, fmt.Sprintf("• %s (%s) - added %s",
				c.UserID, name, c.AddedAt.Format("2006-01-02 15:04")))
		}
	} else {
		lines = append(lines, "No collaborators")
	}

	invites, err := h.inviteStore.ListInvites(ctx)
	if err != nil {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "Failed to list invites. Please try again."); err != nil {
			return err
		}
		return nil
	}

	if len(invites) > 0 {
		lines = append(lines, "", "Active Invites:")
		for _, inv := range invites {
			lines = append(lines, fmt.Sprintf("expires %s", inv.ExpiresAt.Format("2006-01-02 15:04")))
		}
	}

	message := strings.Join(lines, "\n")
	if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, message); err != nil {
		return err
	}
	return nil
}

func (h *userHandler) onRemove(ctx context.Context, commandCtx baldatelegram.CommandContext) error {
	args := strings.Fields(commandCtx.Args)
	if len(args) < 2 {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "Usage: /user remove <user_id>"); err != nil {
			return err
		}
		return nil
	}

	userID := strings.TrimSpace(args[1])
	if userID == "" {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "User ID required"); err != nil {
			return err
		}
		return nil
	}

	if err := h.collaboratorStore.RemoveCollaborator(ctx, userID); err != nil {
		if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, "Could not remove collaborator. Please try again."); err != nil {
			return err
		}
		return nil
	}

	message := fmt.Sprintf("Collaborator removed: %s", userID)
	if err := sendPlain(ctx, h.actorDispatcher, userHandlerActorAddress, commandCtx.Locator, message); err != nil {
		return err
	}
	return nil
}
