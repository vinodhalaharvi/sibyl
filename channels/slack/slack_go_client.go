package slack

import (
	"context"
	"fmt"

	slackgo "github.com/slack-go/slack"
)

// NewSlackGoClient constructs a Client backed by the slack-go SDK.
//
// botToken is a Slack bot user token ("xoxb-..."). Required scopes:
//
//	chat:write       - Post messages
//	reactions:read   - Read reactions on messages
//	users:read       - Look up user display names (optional; lookup
//	                    gracefully degrades to passthrough identities)
//
// Optional but recommended:
//
//	chat:write.public - Post to public channels the bot isn't a member of
//	users:read.email  - Include email in resolved Identity.Raw
func NewSlackGoClient(botToken string) Client {
	return &slackGoClient{api: slackgo.New(botToken)}
}

type slackGoClient struct {
	api *slackgo.Client
}

func (c *slackGoClient) Post(ctx context.Context, channel string, opts PostOptions) (string, error) {
	msgOpts := []slackgo.MsgOption{slackgo.MsgOptionText(opts.Text, false)}
	if len(opts.Blocks) > 0 {
		blocks := translateBlocks(opts.Blocks)
		msgOpts = append(msgOpts, slackgo.MsgOptionBlocks(blocks...))
	}
	_, ts, err := c.api.PostMessageContext(ctx, channel, msgOpts...)
	if err != nil {
		return "", fmt.Errorf("slack post: %w", err)
	}
	return ts, nil
}

func (c *slackGoClient) GetReactions(ctx context.Context, channel, ts string) ([]Reaction, error) {
	ref := slackgo.NewRefToMessage(channel, ts)
	rs, err := c.api.GetReactionsContext(ctx, ref, slackgo.NewGetReactionsParameters())
	if err != nil {
		return nil, fmt.Errorf("slack reactions.get: %w", err)
	}
	out := make([]Reaction, 0, len(rs))
	for _, r := range rs {
		out = append(out, Reaction{
			Name:  r.Name,
			Users: r.Users,
			Count: r.Count,
		})
	}
	return out, nil
}

func (c *slackGoClient) GetUserInfo(ctx context.Context, userID string) (UserInfo, error) {
	u, err := c.api.GetUserInfoContext(ctx, userID)
	if err != nil {
		return UserInfo{}, fmt.Errorf("slack users.info: %w", err)
	}
	return UserInfo{
		ID:          u.ID,
		DisplayName: u.Profile.DisplayName,
		Email:       u.Profile.Email,
		RealName:    u.RealName,
	}, nil
}

// translateBlocks converts the adapter's neutral Block representation into
// slack-go's Block types. Unsupported block kinds are skipped silently.
func translateBlocks(blocks []Block) []slackgo.Block {
	out := make([]slackgo.Block, 0, len(blocks))
	for _, b := range blocks {
		switch b.Kind {
		case BlockHeader:
			out = append(out, slackgo.NewHeaderBlock(
				slackgo.NewTextBlockObject("plain_text", b.Text, true, false),
			))
		case BlockSection:
			out = append(out, slackgo.NewSectionBlock(
				slackgo.NewTextBlockObject("mrkdwn", b.Text, false, false),
				nil, nil,
			))
		case BlockContext:
			els := make([]slackgo.MixedElement, 0, len(b.Elements))
			for _, e := range b.Elements {
				els = append(els, slackgo.NewTextBlockObject("mrkdwn", e, false, false))
			}
			if len(els) > 0 {
				out = append(out, slackgo.NewContextBlock("", els...))
			}
		case BlockActions:
			els := make([]slackgo.BlockElement, 0, len(b.Buttons))
			for _, btn := range b.Buttons {
				if btn.URL != "" {
					be := slackgo.NewButtonBlockElement(btn.ActionID, "", slackgo.NewTextBlockObject("plain_text", btn.Label, true, false))
					be.URL = btn.URL
					if btn.Style != "" {
						be.Style = slackgo.Style(btn.Style)
					}
					els = append(els, be)
				} else {
					be := slackgo.NewButtonBlockElement(btn.ActionID, btn.ActionID, slackgo.NewTextBlockObject("plain_text", btn.Label, true, false))
					if btn.Style != "" {
						be.Style = slackgo.Style(btn.Style)
					}
					els = append(els, be)
				}
			}
			if len(els) > 0 {
				out = append(out, slackgo.NewActionBlock("", els...))
			}
		}
	}
	return out
}
