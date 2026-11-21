export type SidebarMode = "work" | "code";

export interface SidebarModeSelection {
  readonly routeKey: string | null;
  readonly routeMode: SidebarMode | null;
  readonly mode: SidebarMode;
}

export function followSidebarThreadMode(
  selection: SidebarModeSelection,
  routeKey: string | null,
  routeMode: SidebarMode | null,
): SidebarModeSelection {
  if (selection.routeKey === routeKey && selection.routeMode === routeMode) return selection;
  return { routeKey, routeMode, mode: routeMode ?? selection.mode };
}
