import { useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { ThreadId, type EnvironmentId } from "@t3tools/contracts";
import { useRouter } from "@tanstack/react-router";
import { AsyncResult } from "effect/reactivity";
import { PinIcon, SearchIcon } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { useThreadShell } from "../../state/entities";
import { semanggiCatalog, semanggiOpenSource, semanggiSearch } from "../../state/semanggi";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { buildThreadRouteParams } from "../../threadRoutes";
import { Button } from "../ui/button";
import { Dialog, DialogDescription, DialogPopup, DialogTitle } from "../ui/dialog";
import { SidebarInput } from "../ui/sidebar";
import {
  buildBreadcrumb,
  dedupeSearchMatches,
  hasUnreadCompletion,
  type SearchRow,
} from "./semanggiSearchModel";

type ModeFilter = "all" | "work" | "code";

export function SemanggiSearchDialog(props: {
  readonly environmentId: EnvironmentId;
  /** Restrict results to one Project; null searches every Project. */
  readonly projectId: string | null;
  /** Accessible name of the trigger; distinguishes the global and per-project buttons. */
  readonly label?: string;
}) {
  const { environmentId, projectId } = props;
  const triggerLabel = props.label ?? "Search Semanggi threads";
  const [open, setOpen] = useState(false);
  const [text, setText] = useState("");
  const [debounced, setDebounced] = useState("");
  const [mode, setMode] = useState<ModeFilter>("all");
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- debounce timer keeps the RPC query in step with typing without a request per keystroke
  useEffect(() => {
    const handle = setTimeout(() => setDebounced(text.trim()), 200);
    return () => clearTimeout(handle);
  }, [text]);

  const query = open && debounced.length > 0 ? debounced : null;
  const searchAtom = semanggiSearch({
    environmentId,
    input: { environmentId, projectId, query: query ?? "_", limit: 50 },
  });
  const searchResult = useAtomValue(searchAtom);
  const catalog = AsyncResult.getOrElse(
    useAtomValue(semanggiCatalog({ environmentId, input: {} })),
    () => null,
  );
  const value = query === null ? null : AsyncResult.getOrElse(searchResult, () => null);
  const failed = query !== null && AsyncResult.isFailure(searchResult);
  const rows = value ? dedupeSearchMatches(value.matches, mode) : [];
  const safeActive = Math.min(active, Math.max(rows.length - 1, 0));

  const router = useRouter();
  const openSource = useOrchestrationCommand(semanggiOpenSource, { reportFailure: true });
  const openRow = (row: SearchRow) => {
    void openSource({
      environmentId,
      input: {
        environmentId,
        projectId,
        sessionId: row.sessionId,
        ...(row.messageId ? { messageId: row.messageId } : {}),
      },
    }).then((result) => {
      if (!result) return;
      setOpen(false);
      void router.navigate({
        to: "/$environmentId/$threadId",
        params: buildThreadRouteParams(
          scopeThreadRef(environmentId, ThreadId.make(result.threadId)),
        ),
      });
    });
  };

  const breadcrumbFor = (row: SearchRow) => {
    const project = catalog?.projects.find((candidate) => candidate.id === row.projectId);
    const sections =
      catalog?.sectionsByProject.find((entry) => entry.projectId === row.projectId)?.sections ?? [];
    const sectionId =
      catalog?.sessions.find((session) => session.sessionId === row.sessionId)?.sectionId ?? null;
    return buildBreadcrumb(project?.name ?? "Unknown project", sections, sectionId).join(" › ");
  };

  const offline = typeof navigator !== "undefined" && navigator.onLine === false;
  const status =
    query === null
      ? "Type to search threads."
      : offline
        ? "Offline. Search needs a connection to this environment."
        : failed
          ? "Search failed. Try again."
          : searchResult.waiting && value === null
            ? "Searching…"
            : rows.length === 0
              ? "No matching threads."
              : null;

  return (
    <>
      <Button
        variant="ghost-muted"
        size="xs"
        aria-label="Search Semanggi threads"
        onClick={() => setOpen(true)}
      >
        <SearchIcon /> Search
      </Button>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) {
            setText("");
            setDebounced("");
            setActive(0);
          }
        }}
      >
        <DialogPopup initialFocus={inputRef}>
          <DialogTitle>Search threads</DialogTitle>
          <DialogDescription>
            {projectId === null ? "Across all projects" : "Within this project"}
            {value && value.syncState !== "LIVE"
              ? ` · index ${value.syncState.toLowerCase()}, results may be incomplete`
              : ""}
          </DialogDescription>
          <div className="flex gap-1 px-4 pb-2" role="group" aria-label="Mode filter">
            {(["all", "work", "code"] as const).map((candidate) => (
              <Button
                key={candidate}
                size="xs"
                variant={mode === candidate ? "secondary" : "ghost-muted"}
                aria-pressed={mode === candidate}
                onClick={() => setMode(candidate)}
              >
                {candidate}
              </Button>
            ))}
          </div>
          <div className="px-4 pb-2">
            <SidebarInput
              ref={inputRef}
              nativeInput
              type="search"
              aria-label={triggerLabel}
              role="combobox"
              aria-expanded={rows.length > 0}
              aria-controls="semanggi-search-results"
              aria-activedescendant={
                rows.length > 0 ? `semanggi-search-result-${safeActive}` : undefined
              }
              value={text}
              onChange={(event) => {
                setText(event.currentTarget.value);
                setActive(0);
              }}
              onKeyDown={(event) => {
                if (event.key === "ArrowDown") {
                  event.preventDefault();
                  setActive(Math.min(safeActive + 1, rows.length - 1));
                } else if (event.key === "ArrowUp") {
                  event.preventDefault();
                  setActive(Math.max(safeActive - 1, 0));
                } else if (event.key === "Enter") {
                  const row = rows[safeActive];
                  if (row) {
                    event.preventDefault();
                    openRow(row);
                  }
                }
              }}
              placeholder="Search"
            />
          </div>
          <div role="status" aria-live="polite" className="px-4 pb-2 text-xs text-muted-foreground">
            {status}
          </div>
          <ul
            id="semanggi-search-results"
            role="listbox"
            aria-label="Search results"
            className="max-h-80 overflow-y-auto px-2 pb-3"
          >
            {rows.map((row, index) => (
              <SearchResultItem
                key={row.sessionId}
                id={`semanggi-search-result-${index}`}
                environmentId={environmentId}
                row={row}
                breadcrumb={breadcrumbFor(row)}
                selected={index === safeActive}
                onSelect={() => openRow(row)}
                onHover={() => setActive(index)}
              />
            ))}
          </ul>
        </DialogPopup>
      </Dialog>
    </>
  );
}

function SearchResultItem(props: {
  readonly id: string;
  readonly environmentId: EnvironmentId;
  readonly row: SearchRow;
  readonly breadcrumb: string;
  readonly selected: boolean;
  readonly onSelect: () => void;
  readonly onHover: () => void;
}) {
  const { row } = props;
  const thread = useThreadShell(scopeThreadRef(props.environmentId, ThreadId.make(row.threadId)));
  return (
    <li
      id={props.id}
      role="option"
      aria-selected={props.selected}
      className={`flex cursor-pointer flex-col rounded-md px-2 py-1.5 text-sm ${props.selected ? "bg-accent" : ""}`}
      onClick={props.onSelect}
      onMouseEnter={props.onHover}
    >
      <span className="flex items-center gap-1.5">
        {thread?.pinnedAt != null ? <PinIcon className="size-3" aria-label="Pinned" /> : null}
        {hasUnreadCompletion(thread?.latestRun?.completedAt, thread?.lastVisitedAt) ? (
          <span role="img" aria-label="Unread" className="size-1.5 rounded-full bg-primary" />
        ) : null}
        <span className="min-w-0 flex-1 truncate">{thread?.title ?? row.title}</span>
        <span className="text-3xs uppercase text-muted-foreground">{row.mode}</span>
      </span>
      <span className="truncate text-xs text-muted-foreground">
        {props.breadcrumb} · {row.sources.join(", ")}
        {row.stale ? " · may be out of date" : ""}
      </span>
    </li>
  );
}
