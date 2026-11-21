import { EnvironmentId } from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/reactivity";
import * as Cause from "effect/Cause";
import { describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { SemanggiOrganization } from "./SemanggiOrganization";
import { SemanggiSearchDialog } from "./SemanggiSearchDialog";

const environmentId = EnvironmentId.make("env");
const mutate = vi.hoisted(() => vi.fn());
const openSource = vi.hoisted(() => vi.fn());
const runSearch = vi.hoisted(() => vi.fn());
const state = vi.hoisted(() => ({
  catalog: null as unknown,
}));

vi.mock("../../state/semanggi", () => ({
  semanggiCatalog: () => state.catalog,
  semanggiSearch: "search",
  semanggiMutate: "mutate",
  semanggiSessionMove: "move",
  semanggiOpenSource: "open",
}));
vi.mock("../../state/use-orchestration-command", () => ({
  useOrchestrationCommand: () => mutate,
}));
vi.mock("../../state/use-atom-command", () => ({
  useAtomCommand: (command: string) => (command === "open" ? openSource : runSearch),
}));
vi.mock("../../state/entities", () => ({
  useThreadShell: () => null,
  useProjects: () => [
    { id: "t3-a", environmentId: "env", title: "Native A", archivedAt: null },
    { id: "t3-b", environmentId: "env", title: "Native B", archivedAt: null },
  ],
}));

const found = (matches: object[]) =>
  AsyncResult.success({ matches, coverage: "partial", syncState: "LIVE" });

const catalog = {
  projects: [
    { id: "unassigned", name: "Unassigned", revision: 1 },
    { id: "p1", name: "Alpha", revision: 1 },
  ],
  workspaceLinks: [{ projectId: "p1", t3ProjectId: "t3-a", isDefault: true }],
  sectionsByProject: [
    {
      projectId: "p1",
      sections: [
        { id: "s1", parentId: null, name: "First", depth: 1, revision: 1 },
        { id: "s2", parentId: null, name: "Second", depth: 1, revision: 1 },
      ],
    },
  ],
  sessions: [
    {
      sessionId: "ses1",
      projectId: "p1",
      sectionId: "s1",
      mode: "code",
      revision: 3,
      threadId: "th1",
    },
  ],
};

function resetState() {
  mutate.mockReset().mockResolvedValue(AsyncResult.success({ revision: 2 }));
  openSource.mockReset();
  state.catalog = Atom.make(AsyncResult.success(catalog));
  runSearch.mockReset().mockResolvedValue(found([]));
}

describe("SemanggiOrganization", () => {
  it("protects Unassigned and sends ordering/membership/workspace mutations", async () => {
    resetState();
    await using screen = await render(<SemanggiOrganization environmentId={environmentId} />);
    await expect.element(screen.getByText("Alpha")).toBeVisible();
    expect(screen.getByRole("button", { name: "Rename Unassigned" }).elements()).toHaveLength(0);
    expect(screen.getByRole("button", { name: "Archive Unassigned" }).elements()).toHaveLength(0);

    await userEvent.click(screen.getByRole("button", { name: "Move Second down" }));
    expect(mutate).toHaveBeenLastCalledWith({
      environmentId,
      input: expect.objectContaining({
        type: "section.reorder",
        id: "s2",
        direction: "down",
        expectedRevision: 1,
      }),
    });

    await userEvent.click(
      screen.getByRole("button", { name: "Unlink Native A from this project" }),
    );
    expect(mutate).toHaveBeenLastCalledWith({
      environmentId,
      input: expect.objectContaining({
        type: "workspace.unlink",
        projectId: "p1",
        t3ProjectId: "t3-a",
      }),
    });

    await userEvent.selectOptions(screen.getByLabelText("Link a workspace to project p1"), "t3-b");
    expect(mutate).toHaveBeenLastCalledWith({
      environmentId,
      input: expect.objectContaining({
        type: "workspace.link",
        t3ProjectId: "t3-b",
        isDefault: false,
      }),
    });
  });

  it("lists the thread under its section and moves it with the session revision", async () => {
    resetState();
    await using screen = await render(<SemanggiOrganization environmentId={environmentId} />);
    await userEvent.selectOptions(screen.getByLabelText("Move Untitled thread to section"), "s2");
    expect(mutate).toHaveBeenLastCalledWith({
      environmentId,
      input: expect.objectContaining({
        expectedRevision: 3,
        operation: { type: "session.move", sessionId: "ses1", projectId: "p1", sectionId: "s2" },
      }),
    });
  });
});

describe("SemanggiSearchDialog", () => {
  const match = (over: object) => ({
    sessionId: "ses1",
    threadId: "th1",
    projectId: "p1",
    mode: "code",
    title: "Deploy plan",
    source: "title",
    freshness: "fresh",
    ...over,
  });

  it("shows empty, results with breadcrumb, and opens the active result with Enter", async () => {
    resetState();
    runSearch.mockResolvedValue(
      found([match({}), match({ source: "assistant", messageId: "m1" })]),
    );
    openSource.mockResolvedValue(AsyncResult.success({ threadId: "th1" }));
    await using screen = await render(
      <SemanggiSearchDialog environmentId={environmentId} projectId={null} label="Search all" />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Search all" }));
    const input = screen.getByRole("combobox", { name: "Search all" });
    await expect.element(input).toHaveFocus();
    await expect.element(screen.getByText("Type to search threads.")).toBeVisible();

    await userEvent.type(input, "deploy");
    await expect.element(screen.getByRole("option", { name: /Deploy plan/ })).toBeVisible();
    await expect.element(screen.getByText(/Alpha › First/)).toBeVisible();
    expect(screen.getByRole("option").elements()).toHaveLength(1); // deduped per session
    await userEvent.keyboard("{Enter}");
    await vi.waitFor(() => expect(openSource).toHaveBeenCalledTimes(1));
    expect(openSource.mock.calls[0]![0].input).toEqual(
      expect.objectContaining({ sessionId: "ses1", projectId: null }),
    );
  });

  it("explains no matches and failures", async () => {
    resetState();
    await using screen = await render(
      <SemanggiSearchDialog environmentId={environmentId} projectId="p1" label="Search Alpha" />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Search Alpha" }));
    await userEvent.type(screen.getByRole("combobox", { name: "Search Alpha" }), "zzz");
    await expect.element(screen.getByText("No matching threads.")).toBeVisible();
    runSearch.mockResolvedValue(AsyncResult.failure(Cause.fail("boom")));
    await userEvent.type(screen.getByRole("combobox", { name: "Search Alpha" }), "z");
    await expect.element(screen.getByText("Search failed. Try again.")).toBeVisible();
  });

  it("moves the active option with arrow keys and explains the offline state", async () => {
    resetState();
    runSearch.mockResolvedValue(
      found([match({}), match({ sessionId: "ses2", threadId: "th2", title: "Second thread" })]),
    );
    using offline = vi.spyOn(navigator, "onLine", "get").mockReturnValue(false);
    await using screen = await render(
      <SemanggiSearchDialog environmentId={environmentId} projectId={null} label="Search all" />,
    );
    await userEvent.click(screen.getByRole("button", { name: "Search all" }));
    const input = screen.getByRole("combobox", { name: "Search all" });
    await userEvent.type(input, "thread");
    await expect.element(screen.getByText(/Offline/)).toBeVisible();
    offline.mockReturnValue(true);
    await userEvent.type(input, "s");
    const options = screen.getByRole("option");
    await expect.element(options.nth(0)).toHaveAttribute("aria-selected", "true");
    await userEvent.keyboard("{ArrowDown}");
    await expect.element(options.nth(1)).toHaveAttribute("aria-selected", "true");
    await userEvent.keyboard("{ArrowUp}");
    await expect.element(options.nth(0)).toHaveAttribute("aria-selected", "true");
  });
});
