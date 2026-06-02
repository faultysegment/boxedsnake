package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
	"github.com/faultysegment/boxedsnake/api/gen/tasks/v1/tasksv1connect"
	"connectrpc.com/connect"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type state int

const (
	stateInput state = iota
	stateLoading
	stateResult
)

type model struct {
	client    tasksv1connect.TaskServiceClient
	state     state
	textarea  textarea.Model
	spinner   spinner.Model
	err       error
	result    *tasksv1.SubmitTaskResponse
}

type resultMsg struct {
	res *tasksv1.SubmitTaskResponse
	err error
}

func initialModel() model {
	ti := textarea.New()
	ti.SetValue("def run():\n    print('Hello World from run()!')\n    return 'ok'\n\ndef send_result():\n    import json, os\n    with open(os.environ['BOXED_SNAKE_OUTPUT_FILE'], 'w') as f:\n        json.dump({'status': 'success'}, f)")
	ti.Focus()
	ti.CharLimit = 10000
	ti.SetWidth(80)
	ti.SetHeight(20)
	ti.ShowLineNumbers = true

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return model{
		client:   tasksv1connect.NewTaskServiceClient(http.DefaultClient, "http://localhost:8080"),
		state:    stateInput,
		textarea: ti,
		spinner:  s,
	}
}

func (m model) Init() tea.Cmd {
	return textarea.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		case tea.KeyEsc:
			if m.state == stateResult {
				m.state = stateInput
				m.result = nil
				m.err = nil
				return m, nil
			}
		case tea.KeyCtrlS:
			if m.state == stateInput {
				m.state = stateLoading
				script := m.textarea.Value()
				return m, tea.Batch(m.spinner.Tick, runScript(m.client, script))
			}
		}

	case resultMsg:
		m.state = stateResult
		m.result = msg.res
		m.err = msg.err
		return m, nil

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	}

	var cmd tea.Cmd
	if m.state == stateInput {
		m.textarea, cmd = m.textarea.Update(msg)
	}
	return m, cmd
}

func (m model) View() string {
	switch m.state {
	case stateInput:
		return fmt.Sprintf(
			"\n  🐍 Boxed Snake Client (TUI)\n\n%s\n\n  %s",
			lipgloss.NewStyle().MarginLeft(2).Render(m.textarea.View()),
			lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render("Ctrl+S to submit • Ctrl+C to quit"),
		)
	case stateLoading:
		return fmt.Sprintf("\n\n  %s Executing script in Boxed Snake cluster...\n\n", m.spinner.View())
	case stateResult:
		if m.err != nil {
			return fmt.Sprintf("\n  ❌ Error: %v\n\n  Press Esc to go back.", m.err)
		}
		
		res := m.result
		statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Bold(true)
		if res.Status != "success" {
			statusStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
		}
		
		var b strings.Builder
		b.WriteString(fmt.Sprintf("📋 Task ID: %s\n", res.TaskId))
		b.WriteString(fmt.Sprintf("✨ Status: %s\n", statusStyle.Render(res.Status)))
		
		if res.Stdout != "" {
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Bold(true).Render("\n--- Stdout ---\n"))
			b.WriteString(res.Stdout)
		}
		
		if res.Stderr != "" {
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true).Render("\n--- Stderr ---\n"))
			b.WriteString(res.Stderr)
		}
		
		if res.ResultData != "" {
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("220")).Bold(true).Render("\n--- Result JSON ---\n"))
			b.WriteString(res.ResultData)
		}
		
		b.WriteString("\n\n")
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render("Press Esc to write a new script • Ctrl+C to quit"))
		
		return lipgloss.NewStyle().Margin(1, 2).Render(b.String())
	}
	return ""
}

func runScript(client tasksv1connect.TaskServiceClient, script string) tea.Cmd {
	return func() tea.Msg {
		req := connect.NewRequest(&tasksv1.SubmitTaskRequest{
			ScriptContent: script,
			EnvVars:       map[string]string{},
			TimeoutSeconds: 30,
		})
		
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		
		res, err := client.ExecuteTask(ctx, req)
		if err != nil {
			return resultMsg{err: err}
		}
		return resultMsg{res: res.Msg}
	}
}

func main() {
	p := tea.NewProgram(initialModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
