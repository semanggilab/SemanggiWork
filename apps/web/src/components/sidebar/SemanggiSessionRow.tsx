import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { ThreadId, type EnvironmentId } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { MessageSquareIcon } from "lucide-react";
import { randomUUID } from "../../lib/utils";
import { useThreadShell } from "../../state/entities";
import { buildThreadRouteParams } from "../../threadRoutes";
import { hasUnreadCompletion } from "./semanggiSearchModel";

export interface SemanggiSessionRowProps {
  readonly environmentId: EnvironmentId;
  readonly projectId: string;
  readonly session: {
    readonly sessionId: string;
    readonly threadId: string;
    readonly sectionId: string | null;
    readonly revision: number;
  };
  readonly sections: ReadonlyArray<{ readonly id: string; readonly name: string }>;
  readonly onMove: (input: {
    readonly requestId: string;
    readonly idempotencyKey: string;
    readonly scope: { readonly environmentId: string; readonly projectId: string };
    readonly expectedRevision: number;
    readonly operation: {
      readonly type: "session.move";
      readonly sessionId: string;
      readonly projectId: string;
      readonly sectionId: string | null;
    };
  }) => void;
}

/** One Semanggi session: a link to its native T3 thread plus a "move to section" control. */
export function SemanggiSessionRow(props: SemanggiSessionRowProps) {
  const { environmentId, projectId, session, sections, onMove } = props;
  const threadRef = scopeThreadRef(environmentId, ThreadId.make(session.threadId));
  const thread = useThreadShell(threadRef);
  const title = thread?.title ?? "Untitled thread";
  const unread = hasUnreadCompletion(thread?.latestRun?.completedAt, thread?.lastVisitedAt);
  return (
    <div className="flex items-center gap-1.5 py-0.5 text-sidebar-foreground">
      <MessageSquareIcon className="size-3 shrink-0" />
      <Link
        className="min-w-0 flex-1 truncate hover:underline"
        to="/$environmentId/$threadId"
        params={buildThreadRouteParams(threadRef)}
      >
        {title}
      </Link>
      {unread ? (
        <span
          role="img"
          aria-label="Unread"
          className="size-1.5 shrink-0 rounded-full bg-primary"
        />
      ) : null}
      <select
        aria-label={`Move ${title} to section`}
        className="max-w-16 bg-transparent text-3xs text-sidebar-muted-foreground"
        value=""
        onChange={(event) => {
          const value = event.currentTarget.value;
          if (value === "") return;
          const sectionId = value === "__root" ? null : value;
          if (sectionId === session.sectionId) return;
          onMove({
            requestId: randomUUID(),
            idempotencyKey: randomUUID(),
            scope: { environmentId, projectId },
            expectedRevision: session.revision,
            operation: { type: "session.move", sessionId: session.sessionId, projectId, sectionId },
          });
        }}
      >
        <option value="">Move</option>
        <option value="__root">Project root</option>
        {sections.map((section) => (
          <option key={section.id} value={section.id}>
            {section.name}
          </option>
        ))}
      </select>
    </div>
  );
}
