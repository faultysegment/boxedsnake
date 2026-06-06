package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	tasksv1 "github.com/faultysegment/boxedsnake/api/gen/tasks/v1"
	"github.com/faultysegment/boxedsnake/api/gen/tasks/v1/tasksv1connect"

	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type focusPane int

const (
	paneLeft focusPane = iota
	paneRight
)

type rightPaneState int

const (
	rightPaneEmpty rightPaneState = iota
	rightPaneHistoryLoading
	rightPaneHistoryResult
	rightPaneSelectType
	rightPaneInputParam
	rightPaneInputScript
	rightPaneLoading
	rightPaneResult
)

type model struct {
	client tasksv1connect.TaskServiceClient

	focus focusPane
	state rightPaneState

	// Left pane
	taskList list.Model

	// Right pane (Creation)
	taskType tasksv1.TaskType
	paramTi  textinput.Model
	textarea textarea.Model

	// Right pane (Viewing/Loading)
	spinner spinner.Model
	history *tasksv1.GetTaskResultsResponse
	result  *tasksv1.SubmitTaskResponse
	err     error

	rawTasks  []*tasksv1.TaskSummary
	tabs      []string
	activeTab int

	width  int
	height int
}

type taskItem struct {
	id     string
	title  string
	desc   string
	isNew  bool
}

func (i taskItem) Title() string       { return i.title }
func (i taskItem) Description() string { return i.desc }
func (i taskItem) FilterValue() string { return i.id }

// Msgs
type listTasksResultMsg struct {
	res *tasksv1.ListTasksResponse
	err error
}

type historyResultMsg struct {
	res *tasksv1.GetTaskResultsResponse
	err error
}

type submitResultMsg struct {
	res *tasksv1.SubmitTaskResponse
	err error
}

type cancelResultMsg struct {
	res *tasksv1.CancelTaskResponse
	err error
}

func initialModel() model {
	paramTi := textinput.New()
	paramTi.Width = 40

	ti := textarea.New()
	ti.SetValue("def run():\n    print('Hello World from run()!')\n    return 'ok'\n\ndef send_result():\n    import json, os\n    with open(os.environ['BOXED_SNAKE_OUTPUT_FILE'], 'w') as f:\n        json.dump({'status': 'success'}, f)")
	ti.CharLimit = 10000
	ti.SetWidth(80)
	ti.SetHeight(20)
	ti.ShowLineNumbers = true

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	delegate := list.NewDefaultDelegate()
	taskList := list.New([]list.Item{}, delegate, 30, 20)
	taskList.Title = "Tasks"
	taskList.SetShowStatusBar(false)

	return model{
		client:   tasksv1connect.NewTaskServiceClient(http.DefaultClient, "http://localhost:8080"),
		focus:    paneLeft,
		state:    rightPaneEmpty,
		taskList: taskList,
		paramTi:  paramTi,
		textarea: ti,
		spinner:  s,
		tabs:     []string{"All", "Pending", "Success", "Failed"},
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		fetchListTasks(m.client),
	)
}

func (m *model) updateListItems() {
	var items []list.Item
	items = append(items, taskItem{id: "new", title: "➕ Create New Task", desc: "Submit a new script for execution", isNew: true})

	activeFilter := strings.ToLower(m.tabs[m.activeTab])

	for _, t := range m.rawTasks {
		// Filter by tab
		if activeFilter != "all" {
			statusLower := strings.ToLower(t.Status)
			if activeFilter == "success" && statusLower != "success" && statusLower != "completed" {
				continue
			}
			if activeFilter == "failed" && statusLower != "failed" && statusLower != "error" {
				continue
			}
			if activeFilter == "pending" && statusLower != "pending" && statusLower != "scheduled" {
				continue
			}
		}

		tm := time.Unix(t.LastExecutedAt, 0).Format(time.RFC822)
		items = append(items, taskItem{
			id:    t.TaskId,
			title: t.TaskId,
			desc:  fmt.Sprintf("Status: %s | Last: %s", t.Status, tm),
			isNew: false,
		})
	}
	m.taskList.SetItems(items)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.taskList.SetSize(msg.Width/3, msg.Height-4)
		m.textarea.SetWidth(msg.Width*2/3 - 4)
		m.textarea.SetHeight(msg.Height - 10)
		return m, nil

	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			return m, tea.Quit
		}

		if msg.Type == tea.KeyEsc {
			if m.focus == paneRight {
				m.focus = paneLeft
				// if we escape right pane, clear state
				if m.state != rightPaneHistoryResult {
					m.state = rightPaneEmpty
				}
				return m, nil
			}
		}
	}

	if m.focus == paneLeft {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			if msg.Type == tea.KeyTab {
				m.activeTab = (m.activeTab + 1) % len(m.tabs)
				m.updateListItems()
				return m, nil
			} else if msg.Type == tea.KeyShiftTab {
				m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
				m.updateListItems()
				return m, nil
			} else if msg.Type == tea.KeyEnter {
				selectedItem := m.taskList.SelectedItem()
				if selectedItem != nil {
					item := selectedItem.(taskItem)
					if item.isNew {
						m.state = rightPaneSelectType
						m.focus = paneRight
					} else {
						m.state = rightPaneHistoryLoading
						cmds = append(cmds, fetchHistory(m.client, item.id))
					}
				}
			} else if msg.String() == "x" {
				selectedItem := m.taskList.SelectedItem()
				if selectedItem != nil {
					item := selectedItem.(taskItem)
					if !item.isNew && strings.Contains(item.desc, "PENDING") {
						cmds = append(cmds, cancelTask(m.client, item.id))
					}
				}
			}
		}

		var cmd tea.Cmd
		m.taskList, cmd = m.taskList.Update(msg)
		cmds = append(cmds, cmd)

	} else if m.focus == paneRight {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch m.state {
			case rightPaneSelectType:
				switch msg.String() {
				case "1":
					m.taskType = tasksv1.TaskType_TASK_TYPE_ONE_SHOT
					m.state = rightPaneInputScript
					m.textarea.Focus()
					cmds = append(cmds, textarea.Blink)
				case "2":
					m.taskType = tasksv1.TaskType_TASK_TYPE_POSTPONED
					m.paramTi.Placeholder = "Delay in seconds (e.g., 60)"
					m.paramTi.SetValue("")
					m.paramTi.Focus()
					m.state = rightPaneInputParam
					cmds = append(cmds, textinput.Blink)
				case "3":
					m.taskType = tasksv1.TaskType_TASK_TYPE_SCHEDULED
					m.paramTi.Placeholder = "Cron expression (e.g., * * * * *)"
					m.paramTi.SetValue("")
					m.paramTi.Focus()
					m.state = rightPaneInputParam
					cmds = append(cmds, textinput.Blink)
				}
			case rightPaneInputParam:
				if msg.Type == tea.KeyEnter {
					m.state = rightPaneInputScript
					m.paramTi.Blur()
					m.textarea.Focus()
					cmds = append(cmds, textarea.Blink)
				} else {
					var cmd tea.Cmd
					m.paramTi, cmd = m.paramTi.Update(msg)
					cmds = append(cmds, cmd)
				}
			case rightPaneInputScript:
				if msg.Type == tea.KeyCtrlS {
					m.state = rightPaneLoading
					script := m.textarea.Value()
					var execAt int64
					var cronExpr string

					if m.taskType == tasksv1.TaskType_TASK_TYPE_POSTPONED {
						delay, _ := strconv.Atoi(m.paramTi.Value())
						execAt = time.Now().Add(time.Duration(delay) * time.Second).Unix()
					} else if m.taskType == tasksv1.TaskType_TASK_TYPE_SCHEDULED {
						cronExpr = m.paramTi.Value()
					}
					cmds = append(cmds, runScript(m.client, script, m.taskType, execAt, cronExpr))
				} else {
					var cmd tea.Cmd
					m.textarea, cmd = m.textarea.Update(msg)
					cmds = append(cmds, cmd)
				}
			case rightPaneResult:
				if msg.Type == tea.KeyEnter {
					cmds = append(cmds, fetchListTasks(m.client))
					m.focus = paneLeft
					m.state = rightPaneEmpty
				}
			}
		}
	}

	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)
	case listTasksResultMsg:
		if msg.err == nil {
			m.rawTasks = msg.res.Tasks
			m.updateListItems()
		} else {
			m.err = msg.err
		}
	case historyResultMsg:
		m.history = msg.res
		m.err = msg.err
		m.state = rightPaneHistoryResult
	case submitResultMsg:
		m.result = msg.res
		m.err = msg.err
		m.state = rightPaneResult
		cmds = append(cmds, fetchListTasks(m.client))
	case cancelResultMsg:
		if msg.err != nil {
			m.err = msg.err
			m.state = rightPaneResult // reuse rightPaneResult to show error? Wait, let's just use it or add a small status message.
			// For simplicity, just refresh list if successful, or set err.
		} else if !msg.res.Success {
			m.err = fmt.Errorf(msg.res.Message)
		} else {
			// Refresh list on success
			cmds = append(cmds, fetchListTasks(m.client))
		}
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	leftBorderColor := "238"
	rightBorderColor := "238"

	if m.focus == paneLeft {
		leftBorderColor = "62"
	} else if m.focus == paneRight {
		rightBorderColor = "62"
	}

	var renderedTabs []string
	for i, t := range m.tabs {
		style := lipgloss.NewStyle().Padding(0, 1)
		if i == m.activeTab {
			style = style.Foreground(lipgloss.Color("205")).Bold(true).Underline(true)
		} else {
			style = style.Foreground(lipgloss.Color("240"))
		}
		renderedTabs = append(renderedTabs, style.Render(t))
	}
	tabsRow := lipgloss.JoinHorizontal(lipgloss.Top, renderedTabs...)
	tabsRow = lipgloss.NewStyle().MarginBottom(1).Render(tabsRow)

	leftContent := lipgloss.JoinVertical(lipgloss.Left, tabsRow, m.taskList.View())

	leftPane := lipgloss.NewStyle().
		Width(m.width / 3).
		Height(m.height - 2).
		Border(lipgloss.NormalBorder(), false, true, false, false).
		BorderForeground(lipgloss.Color(leftBorderColor)).
		Render(leftContent)

	var rightContent string
	switch m.state {
	case rightPaneEmpty:
		rightContent = "\n  Select a task on the left or create a new one."
	case rightPaneHistoryLoading, rightPaneLoading:
		rightContent = fmt.Sprintf("\n  %s Communicating with Boxed Snake cluster...", m.spinner.View())
	case rightPaneHistoryResult:
		if m.err != nil {
			rightContent = fmt.Sprintf("\n  ❌ Error: %v", m.err)
		} else if len(m.history.Results) == 0 {
			rightContent = "\n  No history found for this Task ID."
		} else {
			var b strings.Builder
			b.WriteString(fmt.Sprintf("📋 History for Task ID: %s\n\n", m.history.Results[0].TaskId))
			for _, res := range m.history.Results {
				t := time.Unix(res.ExecutedAt, 0).Format(time.RFC822)
				statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Bold(true)
				if res.Status != "success" && res.Status != "COMPLETED" {
					statusStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
				}
				b.WriteString(fmt.Sprintf("[%s] %s\n", t, statusStyle.Render(res.Status)))
				if res.Stdout != "" {
					b.WriteString(fmt.Sprintf("  Stdout: %s\n", strings.TrimSpace(res.Stdout)))
				}
				if res.Stderr != "" {
					b.WriteString(fmt.Sprintf("  Stderr: %s\n", strings.TrimSpace(res.Stderr)))
				}
				if res.ResultData != "" {
					b.WriteString(fmt.Sprintf("  Result: %s\n", strings.TrimSpace(res.ResultData)))
				}
				b.WriteString("\n")
			}
			rightContent = b.String()
		}
	case rightPaneSelectType:
		rightContent = "  Select Task Type:\n  1. One Shot (Run now)\n  2. Postponed (Run once later)\n  3. Scheduled (Cron job)\n\n  Press 1, 2, or 3. (Esc to cancel)"
	case rightPaneInputParam:
		rightContent = fmt.Sprintf("  Enter parameter for %s:\n\n  %s\n\n  Press Enter to continue.", m.taskType, m.paramTi.View())
	case rightPaneInputScript:
		rightContent = lipgloss.NewStyle().MarginLeft(2).Render(m.textarea.View()) +
			"\n\n  " + lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render("Ctrl+S to submit • Esc to cancel")
	case rightPaneResult:
		if m.err != nil {
			rightContent = fmt.Sprintf("\n  ❌ Error: %v\n\n  Press Enter to continue.", m.err)
		} else {
			res := m.result
			statusStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Bold(true)
			if res.Status != "success" && res.Status != "Scheduled" {
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
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render("Press Enter to continue"))
			rightContent = b.String()
		}
	}

	rightPane := lipgloss.NewStyle().
		Width(m.width*2/3 - 4).
		Height(m.height - 2).
		Border(lipgloss.NormalBorder(), false, false, false, false).
		BorderForeground(lipgloss.Color(rightBorderColor)).
		Padding(1, 2).
		Render(rightContent)

	return lipgloss.JoinHorizontal(lipgloss.Top, leftPane, rightPane)
}

func runScript(client tasksv1connect.TaskServiceClient, script string, taskType tasksv1.TaskType, executeAt int64, cronExpr string) tea.Cmd {
	return func() tea.Msg {
		req := connect.NewRequest(&tasksv1.SubmitTaskRequest{
			ScriptContent:  script,
			EnvVars:        map[string]string{},
			TimeoutSeconds: 30,
			TaskType:       taskType,
			ExecuteAt:      executeAt,
			CronExpression: cronExpr,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()

		res, err := client.ExecuteTask(ctx, req)
		if err != nil {
			return submitResultMsg{err: err}
		}
		return submitResultMsg{res: res.Msg}
	}
}

func fetchHistory(client tasksv1connect.TaskServiceClient, taskID string) tea.Cmd {
	return func() tea.Msg {
		req := connect.NewRequest(&tasksv1.GetTaskResultsRequest{
			TaskId: taskID,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		res, err := client.GetTaskResults(ctx, req)
		if err != nil {
			return historyResultMsg{err: err}
		}
		return historyResultMsg{res: res.Msg}
	}
}

func fetchListTasks(client tasksv1connect.TaskServiceClient) tea.Cmd {
	return func() tea.Msg {
		req := connect.NewRequest(&tasksv1.ListTasksRequest{})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		res, err := client.ListTasks(ctx, req)
		if err != nil {
			return listTasksResultMsg{err: err}
		}
		return listTasksResultMsg{res: res.Msg}
	}
}

func cancelTask(client tasksv1connect.TaskServiceClient, taskID string) tea.Cmd {
	return func() tea.Msg {
		req := connect.NewRequest(&tasksv1.CancelTaskRequest{
			TaskId: taskID,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		res, err := client.CancelTask(ctx, req)
		if err != nil {
			return cancelResultMsg{err: err}
		}
		return cancelResultMsg{res: res.Msg}
	}
}

func main() {
	p := tea.NewProgram(initialModel(), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
