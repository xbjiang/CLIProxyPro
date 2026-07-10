package thinking

import (
	"fmt"
	"github.com/tidwall/gjson"
)

// ExtractReasoningLevel extracts the reasoning level/effort configuration from the raw JSON body.
// It supports OpenAI, Codex, Claude, Gemini, Antigravity, and other compatible formats.
func ExtractReasoningLevel(body []byte) string {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}

	// 1. Check OpenAI/Kimi format (reasoning_effort)
	if effort := gjson.GetBytes(body, "reasoning_effort"); effort.Exists() {
		return effort.String()
	}

	// 2. Check Codex format (reasoning.effort)
	if effort := gjson.GetBytes(body, "reasoning.effort"); effort.Exists() {
		return effort.String()
	}

	// 3. Check Claude format (thinking)
	thinkingType := gjson.GetBytes(body, "thinking.type")
	if thinkingType.Exists() {
		t := thinkingType.String()
		if t == "disabled" {
			return "disabled"
		}
		if t == "enabled" {
			budget := gjson.GetBytes(body, "thinking.budget_tokens")
			if budget.Exists() {
				return fmt.Sprintf("budget: %d", budget.Int())
			}
			return "enabled"
		}
		if t == "adaptive" {
			effort := gjson.GetBytes(body, "output_config.effort")
			if effort.Exists() {
				return fmt.Sprintf("adaptive (%s)", effort.String())
			}
			return "adaptive"
		}
		return t
	}

	// 4. Check Gemini / Antigravity / Gemini CLI format (thinkingConfig)
	var tc gjson.Result
	if tc = gjson.GetBytes(body, "generationConfig.thinkingConfig"); !tc.Exists() {
		tc = gjson.GetBytes(body, "request.generationConfig.thinkingConfig")
	}

	if tc.Exists() && tc.IsObject() {
		// Check for thinkingLevel
		if lv := tc.Get("thinkingLevel"); lv.Exists() {
			return lv.String()
		}
		if lv := tc.Get("thinking_level"); lv.Exists() {
			return lv.String()
		}

		// Check for thinkingBudget
		var bg gjson.Result
		if bg = tc.Get("thinkingBudget"); !bg.Exists() {
			bg = tc.Get("thinking_budget")
		}
		if bg.Exists() {
			val := bg.Int()
			if val == -1 {
				return "auto"
			}
			if val == 0 {
				return "disabled"
			}
			return fmt.Sprintf("budget: %d", val)
		}
	}

	return ""
}
