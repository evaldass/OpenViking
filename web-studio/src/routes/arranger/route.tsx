import { createFileRoute } from '@tanstack/react-router'
import { ArrangerPage } from '#/features/arranger/arranger-page'
import type { ArrangerSearch } from '#/features/arranger/arranger-page'

export const Route = createFileRoute('/arranger')({
  validateSearch: (search: Record<string, unknown>): ArrangerSearch => ({
    selected: typeof search.selected === 'string' ? search.selected : undefined,
    uri: typeof search.uri === 'string' ? search.uri : undefined,
    view:
      search.view === 'graph' || search.view === 'list'
        ? search.view
        : undefined,
  }),
  component: ArrangerPage,
})
