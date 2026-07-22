# easy-workflow implementation issues

这些本地 issue 从 [项目说明](../README.md) 拆分，均标记为 `ready-for-agent`。仓库尚未配置远程 issue tracker，因此以 Markdown issue 集合保存；编号同时表示建议发布顺序，不代表所有工作必须串行执行。

| ID | Title | Priority | Blocked by | User stories |
|---|---|---|---|---|
| 001 | [预编译并完整校验 Definition](001-compile-and-validate-definitions.md) | P1 | None | 4, 5, 11–17, 49–50 |
| 002 | [发布并运行不可变 Definition 版本](002-publish-immutable-definition-versions.md) | P1 | 001 | 3, 6–10, 13–14 |
| 003 | [建立 Store adapter 契约测试](003-establish-store-adapter-contract-tests.md) | P1 | None | 30–32, 50 |
| 004 | [实现 PostgreSQL durable Store adapter](004-add-postgresql-durable-store-adapter.md) | P1 | 003 | 27–32, 45–48 |
| 005 | [实现受限 Condition extension](005-add-restricted-condition-extension.md) | P1 | 001, 002 | 22–26 |
| 006 | [支持 rejected outcome 显式路由](006-route-rejected-approval-outcomes.md) | P2 | None | 18–21 |
| 007 | [实现 Instance 撤回](007-withdraw-running-instances.md) | P2 | None | 33–34, 39, 45 |
| 008 | [实现显式退回并深化 command module](008-return-instances-and-deepen-command-execution.md) | P2 | 007 | 35–37, 39, 45, 49–50 |
| 009 | [支持 Approval assignment policy](009-resolve-approval-assignment-policies.md) | P2 | 002 | 40–42 |
| 010 | [建立待办、已办和参与人查询投影](010-add-worklist-and-participation-projections.md) | P2 | 004, 009 | 43–47 |
| 011 | [实现任务转派](011-transfer-active-tasks.md) | P3 | 008, 009, 010 | 38–39, 43, 45 |
| 012 | [交付 P1 可安装版本与分层文档](012-document-and-package-the-p1-release.md) | P1 | 002, 004, 005 | 1–2, 48 |

## Dependency order

1. 001 and 003 can start immediately.
2. 002 follows 001; 004 follows 003.
3. 005 follows 001 and 002.
4. 012 completes the P1 release after 002, 004 and 005.
5. 006 and 007 can proceed independently; 008 follows 007.
6. 009 follows 002; 010 follows 004 and 009.
7. 011 follows 008, 009 and 010.
