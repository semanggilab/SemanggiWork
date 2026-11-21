import type { EnvironmentId } from "@t3tools/contracts";
import { FolderGit2Icon, StarIcon, UnlinkIcon } from "lucide-react";
import { useProjects } from "../../state/entities";
import { Button } from "../ui/button";

export interface WorkspaceLinkRow {
  readonly projectId: string;
  readonly t3ProjectId: string;
  readonly isDefault: boolean;
}

/** Native T3 workspaces linked to one Semanggi Project. Organization only: cwd/permissions stay native. */
export function SemanggiWorkspaceLinks(props: {
  readonly environmentId: EnvironmentId;
  readonly projectId: string;
  readonly links: ReadonlyArray<WorkspaceLinkRow>;
  readonly onLink: (t3ProjectId: string, isDefault: boolean) => void;
  readonly onUnlink: (t3ProjectId: string) => void;
}) {
  const { environmentId, projectId, links, onLink, onUnlink } = props;
  const native = useProjects().filter(
    (project) =>
      project.environmentId === environmentId && project.archivedAt == null && !project.isScratch,
  );
  const titleOf = (id: string) =>
    native.find((project) => project.id === id)?.title ?? "Unavailable workspace";
  const linked = new Set(links.map((link) => link.t3ProjectId));
  const candidates = native.filter((project) => !linked.has(project.id));
  return (
    <div aria-label="Linked workspaces" className="pl-3 text-sidebar-muted-foreground">
      {links.map((link) => (
        <div key={link.t3ProjectId} className="flex items-center gap-1.5 py-0.5">
          <FolderGit2Icon className="size-3 shrink-0" />
          <span className="min-w-0 flex-1 truncate">{titleOf(link.t3ProjectId)}</span>
          {link.isDefault ? (
            <StarIcon className="size-3" aria-label="Default workspace" />
          ) : (
            <Button
              aria-label={`Make ${titleOf(link.t3ProjectId)} the default workspace`}
              variant="ghost-muted"
              size="icon-micro"
              onClick={() => onLink(link.t3ProjectId, true)}
            >
              <StarIcon />
            </Button>
          )}
          <Button
            aria-label={`Unlink ${titleOf(link.t3ProjectId)} from this project`}
            variant="ghost-muted"
            size="icon-micro"
            onClick={() => onUnlink(link.t3ProjectId)}
          >
            <UnlinkIcon />
          </Button>
        </div>
      ))}
      {candidates.length > 0 ? (
        <select
          aria-label={`Link a workspace to project ${projectId}`}
          className="max-w-full bg-transparent text-3xs"
          value=""
          onChange={(event) => {
            const id = event.currentTarget.value;
            if (id) onLink(id, false);
          }}
        >
          <option value="">Link workspace…</option>
          {candidates.map((project) => (
            <option key={project.id} value={project.id}>
              {project.title}
            </option>
          ))}
        </select>
      ) : null}
    </div>
  );
}
