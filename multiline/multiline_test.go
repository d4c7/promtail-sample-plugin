package multiline

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

const (
	pythonLogLine1 = `[2019-08-13 06:58:20,588] ERROR in app: Exception on /graphql [POST]
Traceback (most recent call last):
  File "/srv/fzapi/venv/lib/python3.6/site-packages/flask/app.py", lineMap 2292, in wsgi_app
    response = self.full_dispatch_request()
  File "/srv/fzapi/venv/lib/python3.6/site-packages/flask/app.py", lineMap 1815, in full_dispatch_request
    rv = self.handle_user_exception(e)
AttributeError: 'Exception' object has no attribute 'path'`

	pythonLogLine2 = `[2019-08-13 06:58:20,589] INFO bla`

	javaLogLine1 = `[2019-08-13 22:00:12 GMT] - [main] ERROR c.i.b.w.w.WebAdapterAgent: cycle failed:
java.lang.NumberFormatException: For input string: "-db error"
	at java.lang.NumberFormatException.forInputString(NumberFormatException.java:65)
	at java.lang.Integer.parseInt(Integer.java:580)
Caused by: MidLevelException: LowLevelException
	at Junk.a(Junk.java:11)
	... 1 more`
	javaLogLine2 = `[2019-08-13 22:00:13 GMT] - [main] INFO  c.i.b.w.w.WebAdapterAgent: All services are now up and running`

	aptHistoryLogLine1 = `Start-Date: 2020-05-15  14:46:48
Commandline: /usr/bin/apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold install docker-ce
Install: containerd.io:amd64 (1.2.13-2, automatic), docker-ce:amd64 (5:19.03.8~3-0~ubuntu-bionic), docker-ce-cli:amd64 (5:19.03.8~3-0~ubuntu-bionic, automatic)
End-Date: 2020-05-15  14:47:04`

	aptHistoryLogLine2 = ``

	aptHistoryLogLine3 = `Start-Date: 2020-05-16  06:06:29
Commandline: /usr/bin/unattended-upgrade
Upgrade: apt-transport-https:amd64 (1.6.12, 1.6.12ubuntu0.1)
End-Date: 2020-05-16  06:06:30`
)

type collectHandler struct {
	lines []string
}

func (s *collectHandler) Handle(_ model.LabelSet, _ time.Time, entry string) error {
	s.lines = append(s.lines, entry)
	return nil
}

func TestMultilineModes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config        Config
		logLines      []string
		expectedLines []string
		err           string
	}{
		"continuation mode": {
			Config{
				Mode:       "continue",
				Expression: `(.*)\\$`,
				Separator:  " ",
			},
			[]string{
				`event\`,
				`one`,
				`two`,
				`event\`,
				`three`},
			[]string{
				"event one",
				"two",
				"event three",
			},
			"",
		},

		"continuation mode handling prefix": {
			Config{
				Mode:               "continue",
				Expression:         `(.*)\\$`,
				NextLineExpression: `BLA.\s(.*)$`,
				Separator:          " ",
			},
			[]string{
				`BLA1 event\`,
				`BLA1 one`,
				`BLA2 two`,
				`BLA3 event\`,
				`BLA3 three`},
			[]string{
				"BLA1 event one",
				"BLA2 two",
				"BLA3 event three",
			},
			"",
		},

		"newline mode": {
			Config{
				Mode:       "newline",
				Expression: "^[^ ]",
			},
			[]string{
				`line 1`,
				` subline 1.1`,
				` subline 1.2`,
				`line 2`,
				` subline 2.1`},
			[]string{
				"line 1 subline 1.1 subline 1.2",
				"line 2 subline 2.1",
			},
			"",
		},

		"group mode": {
			Config{
				Mode:       "group",
				Expression: `^(\S+)`,
			},
			[]string{`G:1 event`,
				`G:1 one`,
				`G:2 event`,
				`G:2 two`},
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},

		"group mode unordered": {
			Config{
				Mode:       "unordered_group",
				Expression: `^(\S+)`,
			},
			[]string{`G:1 event`,
				`G:2 event`,
				`G:1 one`,
				`G:2 two`},
			[]string{
				"G:1 event one",
				"G:2 event two",
			},
			"",
		},

		"group mode compound key": {
			Config{
				Mode:       "group",
				Expression: `(G:\S+).*(H:\S+)`,
				Separator:  " ",
			},
			[]string{`1 G:1 event H:2 rest1`,
				`2 G:1 one H:2 rest2`,
				`3 G:2 event H:2 rest3`,
				`4 G:2 two H:2 rest4`},
			[]string{
				"1 G:1 event H:2 rest1 2  one  rest2",
				"3 G:2 event H:2 rest3 4  two  rest4",
			},
			"",
		},

		"java stacktrace": {
			Config{
				Mode:       "newline",
				Expression: `^\[.*] - `,
				Separator:  "\n",
			},
			append(strings.Split(javaLogLine1, "\n"), javaLogLine2),
			[]string{javaLogLine1, javaLogLine2},
			"",
		},
		"python stacktrace": {
			Config{
				Mode:       "newline",
				Expression: `^\[.*]`,
				Separator:  "\n",
			},
			append(strings.Split(pythonLogLine1, "\n"), pythonLogLine2),
			[]string{pythonLogLine1, pythonLogLine2},
			"",
		},
		"apt log history": {
			Config{
				Mode:       "newline",
				Expression: `^$`,
				Separator:  "\n",
			},
			append(append(strings.Split(aptHistoryLogLine1, "\n"), aptHistoryLogLine2), strings.Split(aptHistoryLogLine3, "\n")...),
			[]string{aptHistoryLogLine1, aptHistoryLogLine3},
			"",
		},

		"named line as separator": {
			Config{
				Mode:                "newline",
				Expression:          `^SEP$`,
				FirstLineExpression: `^$`, // remove first line
				Separator:           "\n",
			},
			[]string{
				"line A-1",
				"line A-2",
				"SEP",
				"line B-1",
				"line B-2",
			},
			[]string{
				"line A-1\nline A-2",
				"line B-1\nline B-2",
			},
			"",
		},
	}

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ch := collectHandler{}

			testData.config.IdleDuration = "1000s"
			pl, err := NewMultiLineParser(util.Logger, &testData.config, &ch)
			if err != nil {
				if testData.err != err.Error() {
					t.Fatal(err)
				}
				return
			}

			ls := model.LabelSet{}
			ts := time.Now()
			for _, s := range testData.logLines {
				err = pl.Handle(ls, ts, s)
				if err != nil {
					t.Failed()
				}
			}
			err = pl.Stop()
			if err != nil {
				t.Failed()
			}

			for i, n, cl := 0, len(testData.expectedLines), len(ch.lines); i < n; i++ {
				if i >= cl {
					assert.Fail(t, "<missing line> '"+testData.expectedLines[i]+"'")
				} else {
					assert.Equal(t, testData.expectedLines[i], ch.lines[i])
				}
			}

			for i, n := len(testData.expectedLines), len(ch.lines); i < n; i++ {
				assert.Fail(t, "<unexpected line> '"+ch.lines[i]+"'")
			}
		})
	}
}

func TestMultilineTimeout(t *testing.T) {
	cfg := Config{
		Mode:         "continue",
		Expression:   `(.*)\\$`,
		IdleDuration: "10ms",
	}
	logLines := []string{
		`event \`,
		`one\`,
	}
	ch := collectHandler{}

	pl, err := NewMultiLineParser(util.Logger, &cfg, &ch)
	if err != nil {
		t.Fatal(err)
	}

	ls := model.LabelSet{}
	ts := time.Now()
	for _, s := range logLines {
		err = pl.Handle(ls, ts, s)
		if err != nil {
			t.Failed()
		}
	}
	time.Sleep(100 * time.Millisecond)

	if len(ch.lines) != 1 {
		t.Fail()
	} else {
		assert.Equal(t, ch.lines[0], "event one")
	}

}

func TestMultilineMultiTrackTimeout(t *testing.T) {
	cfg := Config{
		Mode:         "group",
		Expression:   `(K:\S+)`,
		IdleDuration: "10ms",
	}
	logLines := []string{
		`K:1 line1`,
		`K:2 line2`,
		`K:3 line3`,
	}
	ch := collectHandler{}

	pl, err := NewMultiLineParser(util.Logger, &cfg, &ch)
	if err != nil {
		t.Fatal(err)
	}

	ls := model.LabelSet{}
	ts := time.Now()
	for _, s := range logLines {
		time.Sleep(15 * time.Millisecond)
		err = pl.Handle(ls, ts, s)
		if err != nil {
			t.Failed()
		}
	}
	if len(ch.lines) != 2 {
		t.Fatal("no 2 lines")
	} else {
		assert.Equal(t, ch.lines[0], "K:1 line1")
		assert.Equal(t, ch.lines[1], "K:2 line2")
	}
	time.Sleep(15 * time.Millisecond)
	if len(ch.lines) != 3 {
		t.Fatal("no 3 lines")
	} else {
		assert.Equal(t, ch.lines[2], "K:3 line3")
	}
}

func TestMultilineDisjoint(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		regexp        string
		entry         string
		expectedSel   string
		expectedUnSel string
	}{
		"t1": {
			`F:(\S+\s*)`,
			"F:1 F:2",
			"1 2",
			"F:F:"},

		"t2": {
			`(F:\S+\s*).*(H:\S+\s*)`,
			"E:1 F:1 G:1 H:1",
			"F:1 H:1",
			"E:1 G:1 "},

		"t3": {
			`(F:\S+\s*)+|(H:\S+\s*)+`,
			"E:1 F:1 G:1 H:1 E:2 F:2 G:2 H:2",
			"F:1 H:1 F:2 H:2",
			"E:1 G:1 E:2 G:2 "},

		"t4": {
			`((F:\S+\s*).*(H:\S+\s*))*`,
			"E:1 F:1 G:1 H:1 I:1 E:2 F:2 G:2 H:2 I:2",
			"F:1 G:1 H:1 I:1 E:2 F:2 G:2 H:2 ",
			"E:1 I:2"},
		"continue example": {
			`(.*)\\$`,
			`this line continue\`,
			"this line continue",
			`\`},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			exp, err := regexp.Compile(testData.regexp)
			if err != nil {
				t.Fatal(err)
			}
			sel, inv := disjoint(exp, testData.entry)
			assert.Equal(t, testData.expectedSel, sel)
			assert.Equal(t, testData.expectedUnSel, inv)
		})
	}
}
