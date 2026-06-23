import unittest

from src.agent.planner import _build_planner_prompt, _extract_user_sql
from src.agent.planner_skills import SKILLS, select_planner_skill, validate_all_skills
from src.agent.prompt_compiler import build_prompt_context


class PlannerSkillSelectionTests(unittest.TestCase):
    def test_common_demo_questions_select_expected_skills(self):
        cases = [
            ("近 7 天的 GMV 趋势怎么样，有没有异常波动", "metric_trend"),
            ("5 月 14 日 GMV 下降的原因是什么", "root_cause_analysis"),
            ("数据链路报错了，帮我排查一下", "pipeline_diagnosis"),
            ("order_sync_job 这个 Flink 任务现在状态怎么样，checkpoint 延迟大吗", "pipeline_diagnosis"),
            ("这个月的告警主要集中在哪些服务", "pipeline_diagnosis"),
            ("用户 zhangsan 目前有哪些数据表的查询权限", "metadata"),
            ("orders 表有哪些字段", "metadata"),
            ("查一下昨天各区域的销售额", "data_query"),
        ]

        for query, expected_skill in cases:
            with self.subTest(query=query):
                self.assertEqual(select_planner_skill(query).id, expected_skill)

    def test_sql_error_and_slow_sql_are_distinguished(self):
        error_query = (
            "这条SQL报错，帮我看看什么问题："
            "SELECT region, sum(amount) as gmv FROM orders WHERE order_date_ai = '2024-01-01' GROUP BY region"
        )
        slow_query = (
            "这条SQL执行很慢，帮我看看什么问题："
            "SELECT u.user_id, o.amount FROM users u JOIN orders o ON u.user_id = o.user_id "
            "WHERE o.order_date >= '2024-01-01' ORDER BY o.amount DESC"
        )

        self.assertEqual(select_planner_skill(error_query).id, "sql_diagnosis")
        self.assertEqual(select_planner_skill(slow_query).id, "sql_optimization")

    def test_generic_sql_review_without_error_context_is_not_diagnosis(self):
        """'帮我看看这条SQL' without error keywords should not trigger sql_diagnosis."""
        query = "帮我看看这条SQL：SELECT region FROM orders WHERE order_date = '2024-01-01'"
        skill = select_planner_skill(query)
        self.assertNotEqual(skill.id, "sql_diagnosis")

    def test_sql_log_query_does_not_trigger_pipeline(self):
        """'查看SQL执行日志' should not be routed to pipeline_diagnosis."""
        query = "查看 SQL 执行日志"
        skill = select_planner_skill(query)
        self.assertNotEqual(skill.id, "pipeline_diagnosis")

    def test_english_word_containing_gm_does_not_trigger_metric(self):
        """'algorithm' contains 'gm' but should not match as a metric term."""
        query = "这个 algorithm 趋势怎么样"
        skill = select_planner_skill(query)
        self.assertNotEqual(skill.id, "metric_trend")
        self.assertNotEqual(skill.id, "root_cause_analysis")


class PlannerPromptCompilerTests(unittest.TestCase):
    def test_metric_trend_prompt_contains_trend_rules_but_not_rca_first(self):
        prompt = _build_planner_prompt("近 7 天的 GMV 趋势怎么样，有没有异常波动")

        self.assertIn("skill_id: metric_trend", prompt)
        self.assertIn("metrics 趋势查询规则", prompt)
        self.assertIn("用日期范围代替 LIMIT", prompt)
        self.assertIn("ORDER BY metric_date ASC", prompt)
        self.assertIn("不要直接调用 root_cause_analysis", prompt)

    def test_root_cause_prompt_prefers_root_cause_tool(self):
        prompt = _build_planner_prompt("5 月 14 日 GMV 下降的原因是什么")

        self.assertIn("skill_id: root_cause_analysis", prompt)
        self.assertIn("优先调用 root_cause_analysis", prompt)
        self.assertIn("整体变化", prompt)

    def test_pipeline_prompt_prefers_full_diagnosis(self):
        prompt = _build_planner_prompt("数据链路报错了，帮我排查一下")

        self.assertIn("skill_id: pipeline_diagnosis", prompt)
        self.assertIn("优先调用 pipeline_full_diagnosis", prompt)
        self.assertIn("Flink", prompt)
        self.assertIn("Kafka", prompt)

    def test_metadata_prompt_requires_query_metadata(self):
        prompt = _build_planner_prompt("orders 表有哪些字段")

        self.assertIn("skill_id: metadata", prompt)
        self.assertIn("必须调用 query_metadata", prompt)
        self.assertIn("不要直接查询 system 表", prompt)

    def test_sql_diagnosis_prompt_includes_extracted_sql_and_first_action(self):
        query = (
            "这条SQL报错，帮我看看什么问题：\n"
            "SELECT region, sum(amount) as gmv FROM orders "
            "WHERE order_date_ai BETWEEN '2024-01-01' AND '2024-12-31' "
            "GROUP BY region"
        )
        prompt = _build_planner_prompt(query)

        self.assertIn("skill_id: sql_diagnosis", prompt)
        self.assertIn("SQL 诊断任务（最高优先级）", prompt)
        self.assertIn("SELECT region, sum(amount) as gmv FROM orders", prompt)
        self.assertIn("第一步：用 run_sql 工具执行上面的 SQL", prompt)

    def test_tool_history_is_added_to_prompt(self):
        prompt = _build_planner_prompt(
            "近 7 天的 GMV 趋势怎么样，有没有异常波动",
            tool_results=[
                {
                    "tool": "run_sql",
                    "input": {"query": "SELECT 1"},
                    "output": '{"columns":["x"],"rows":[{"x":1}]}',
                    "success": True,
                },
                {
                    "tool": "run_sql",
                    "input": {"query": "SELECT 1"},
                    "output": '{"columns":["x"],"rows":[{"x":1}]}',
                    "success": True,
                },
            ],
        )

        self.assertIn("已执行的工具调用", prompt)
        self.assertIn("已重复调用 2 次", prompt)
        self.assertIn("不要重复调用", prompt)

    def test_prompt_context_exposes_selected_skill_for_inspection(self):
        ctx = build_prompt_context("这个月的告警主要集中在哪些服务")

        self.assertEqual(ctx.selected_skill.id, "pipeline_diagnosis")
        self.assertIn("pipeline_full_diagnosis", ctx.tools_info)


class SqlExtractionTests(unittest.TestCase):
    def test_extract_user_sql_strips_followup_text(self):
        query = (
            "这条SQL报错，帮我看看什么问题：\n"
            "SELECT region FROM orders WHERE order_date_ai = '2024-01-01'\n"
            "帮我分析一下"
        )

        self.assertEqual(
            _extract_user_sql(query),
            "SELECT region FROM orders WHERE order_date_ai = '2024-01-01'",
        )


class SkillIntegrityTests(unittest.TestCase):
    def test_all_skills_reference_valid_tools(self):
        """Every skill's preferred_tools must exist in the ToolRegistry."""
        errors = validate_all_skills()
        self.assertEqual(errors, [], msg="\n".join(errors))

    def test_all_skills_have_preferred_tools_field(self):
        """Every skill must declare preferred_tools (even if empty)."""
        for skill_id, skill in SKILLS.items():
            self.assertTrue(
                hasattr(skill, "preferred_tools"),
                f"skill '{skill_id}' missing preferred_tools field",
            )
            self.assertIsInstance(
                skill.preferred_tools,
                tuple,
                msg=f"skill '{skill_id}' preferred_tools should be a tuple",
            )

    def test_non_direct_answer_skills_have_at_least_one_preferred_tool(self):
        """All skills except direct_answer should prefer at least one tool."""
        for skill_id, skill in SKILLS.items():
            if skill_id == "direct_answer":
                continue
            self.assertGreater(
                len(skill.preferred_tools),
                0,
                msg=f"skill '{skill_id}' should have at least one preferred tool",
            )


if __name__ == "__main__":
    unittest.main()
