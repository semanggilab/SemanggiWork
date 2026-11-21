import { RegistryContext, useAtomValue } from "@effect/atom-react";
import type { EnvironmentId } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import { ArchiveIcon, FolderIcon, Layers3Icon, PencilIcon, PlusIcon } from "lucide-react";
import { useContext, useState } from "react";
import { semanggiCatalog, semanggiMutate } from "../../state/semanggi";
import { useOrchestrationCommand } from "../../state/use-orchestration-command";
import { Button } from "../ui/button";
import { SidebarInput } from "../ui/sidebar";
import { randomUUID } from "../../lib/utils";

export function SemanggiOrganization({ environmentId }: { environmentId: EnvironmentId }) {
  const catalogAtom = semanggiCatalog({ environmentId, input: {} });
  const result = useAtomValue(catalogAtom);
  const registry = useContext(RegistryContext);
  const mutate = useOrchestrationCommand(semanggiMutate, { reportFailure: true });
  const [newProjectName, setNewProjectName] = useState("");
  const [newSectionProjectId, setNewSectionProjectId] = useState<string | null>(null);
  const [newSectionName, setNewSectionName] = useState("");
  const [editingProjectId, setEditingProjectId] = useState<string | null>(null);
  const [editingProjectName, setEditingProjectName] = useState("");
  const [editingSectionId, setEditingSectionId] = useState<string | null>(null);
  const [editingSectionName, setEditingSectionName] = useState("");
  const [newChildId, setNewChildId] = useState<string | null>(null);
  const [newChildName, setNewChildName] = useState("");
  if (result.waiting) return null;
  const catalog = AsyncResult.getOrElse(result, () => null);
  if (catalog === null || catalog.projects.length === 0) return null;
  const sections = new Map(catalog.sectionsByProject.map((entry) => [entry.projectId, entry.sections]));
  return (
    <div aria-label="Semanggi projects" className="px-2 pb-2 text-xs">
      {catalog.projects.map((project) => (
        <div key={project.id} className="py-1">
          <div className="flex items-center gap-1.5 font-medium text-sidebar-foreground">
            <FolderIcon className="size-3.5" />
            {editingProjectId === project.id ? (
              <form
                className="min-w-0 flex-1"
                onSubmit={(event) => {
                  event.preventDefault();
                  const name = editingProjectName.trim();
                  if (!name) return;
                  void mutate({
                    environmentId,
                    input: { type: "project.rename", requestId: randomUUID(), id: project.id, name, expectedRevision: project.revision },
                  }).then(() => {
                    setEditingProjectId(null);
                    registry.refresh(catalogAtom);
                  });
                }}
              >
                <SidebarInput aria-label={`Rename ${project.name}`} value={editingProjectName} onChange={(event) => setEditingProjectName(event.currentTarget.value)} />
              </form>
            ) : (
              <span className="min-w-0 flex-1 truncate">{project.name}</span>
            )}
            {project.name !== "Unassigned" ? (
              <>
                <Button aria-label={`Rename ${project.name}`} variant="ghost-muted" size="icon-micro" onClick={() => { setEditingProjectId(project.id); setEditingProjectName(project.name); }}><PencilIcon /></Button>
                <Button aria-label={`Archive ${project.name}`} variant="ghost-muted" size="icon-micro" onClick={() => void mutate({ environmentId, input: { type: "project.archive", requestId: randomUUID(), id: project.id, archived: true, expectedRevision: project.revision } }).then(() => registry.refresh(catalogAtom))}><ArchiveIcon /></Button>
              </>
            ) : null}
          </div>
          {sections.get(project.id)?.map((section) => (
            <div key={section.id} style={{ paddingLeft: `${section.depth * 12}px` }}>
              <div className="flex items-center gap-1.5 py-0.5 text-sidebar-muted-foreground">
                <Layers3Icon className="size-3" />
                {editingSectionId === section.id ? (
                  <form className="min-w-0 flex-1" onSubmit={(event) => {
                    event.preventDefault();
                    const name = editingSectionName.trim();
                    if (!name) return;
                    void mutate({ environmentId, input: { type: "section.rename", requestId: randomUUID(), id: section.id, name, expectedRevision: section.revision } }).then(() => { setEditingSectionId(null); registry.refresh(catalogAtom); });
                  }}>
                    <SidebarInput aria-label={`Rename ${section.name}`} value={editingSectionName} onChange={(event) => setEditingSectionName(event.currentTarget.value)} />
                  </form>
                ) : <span className="min-w-0 flex-1 truncate">{section.name}</span>}
                <select
                  aria-label={`Move ${section.name}`}
                  className="max-w-16 bg-transparent text-[10px] text-sidebar-muted-foreground"
                  defaultValue=""
                  onChange={(event) => {
                    const parentId = event.currentTarget.value === "__root" ? null : event.currentTarget.value;
                    if (parentId === section.parentId) return;
                    void mutate({ environmentId, input: { type: "section.move", requestId: randomUUID(), id: section.id, parentId, expectedRevision: section.revision } }).then(() => registry.refresh(catalogAtom));
                  }}
                >
                  <option value="">Move</option>
                  <option value="__root">Root</option>
                  {sections.get(project.id)?.filter((candidate) => candidate.id !== section.id).map((candidate) => <option key={candidate.id} value={candidate.id}>{candidate.name}</option>)}
                </select>
                <Button aria-label={`Add child to ${section.name}`} variant="ghost-muted" size="icon-micro" onClick={() => setNewChildId(section.id)}><PlusIcon /></Button>
                <Button aria-label={`Rename ${section.name}`} variant="ghost-muted" size="icon-micro" onClick={() => { setEditingSectionId(section.id); setEditingSectionName(section.name); }}><PencilIcon /></Button>
                <Button aria-label={`Delete ${section.name}`} variant="ghost-muted" size="icon-micro" onClick={() => void mutate({ environmentId, input: { type: "section.delete", requestId: randomUUID(), id: section.id, expectedRevision: section.revision } }).then(() => registry.refresh(catalogAtom))}><ArchiveIcon /></Button>
              </div>
              {newChildId === section.id ? <form className="flex gap-1 py-0.5" onSubmit={(event) => {
                event.preventDefault(); const name = newChildName.trim(); if (!name) return;
                void mutate({ environmentId, input: { type: "section.create", requestId: randomUUID(), id: randomUUID(), projectId: project.id, parentId: section.id, name } }).then(() => { setNewChildId(null); setNewChildName(""); registry.refresh(catalogAtom); });
              }}><SidebarInput aria-label={`New child section in ${section.name}`} value={newChildName} onChange={(event) => setNewChildName(event.currentTarget.value)} placeholder="New child" /></form> : null}
            </div>
          ))}
          {newSectionProjectId === project.id ? (
            <form
              className="flex gap-1 pt-1"
              onSubmit={(event) => {
                event.preventDefault();
                const name = newSectionName.trim();
                if (!name) return;
                void mutate({
                  environmentId,
                  input: { type: "section.create", requestId: randomUUID(), id: randomUUID(), projectId: project.id, parentId: null, name },
                }).then(() => {
                  setNewSectionName("");
                  setNewSectionProjectId(null);
                  registry.refresh(catalogAtom);
                });
              }}
            >
              <SidebarInput aria-label={`New section in ${project.name}`} value={newSectionName} onChange={(event) => setNewSectionName(event.currentTarget.value)} placeholder="New section" />
            </form>
          ) : (
            <Button variant="ghost-muted" size="xs" onClick={() => setNewSectionProjectId(project.id)}>
              <PlusIcon /> Section
            </Button>
          )}
        </div>
      ))}
      <form
        className="flex gap-1 pt-1"
        onSubmit={(event) => {
          event.preventDefault();
          const name = newProjectName.trim();
          if (!name) return;
          void mutate({
            environmentId,
            input: { type: "project.create", requestId: randomUUID(), id: randomUUID(), name },
          }).then(() => {
            setNewProjectName("");
            registry.refresh(catalogAtom);
          });
        }}
      >
        <SidebarInput aria-label="New Semanggi project" value={newProjectName} onChange={(event) => setNewProjectName(event.currentTarget.value)} placeholder="New project" />
      </form>
    </div>
  );
}
