import json
from datetime import datetime, timezone

from . import config
from .logger import logger


class NotFoundReporter:
    def __init__(self, json_path=None, txt_path=None):
        self.json_path = config.NOT_FOUND_JSON_FILE if json_path is None else json_path
        self.txt_path = config.NOT_FOUND_TXT_FILE if txt_path is None else txt_path

        self.not_imported = {}
        self.records = []

    def ensure_section(self, section):
        if section not in self.not_imported:
            self.not_imported[section] = []

    def record(self, section, item_name, reason, query):
        self.ensure_section(section)
        self.not_imported[section].append(item_name)
        self.records.append(
            {
                'section': section,
                'item': item_name,
                'reason': reason,
                'query': query,
            }
        )

    def group_records(self):
        grouped = {}

        for record in self.records:
            section_name = record['section']
            reason = record['reason']

            if section_name not in grouped:
                grouped[section_name] = {'not_found': [], 'search_error': []}

            if reason not in grouped[section_name]:
                grouped[section_name][reason] = []

            grouped[section_name][reason].append(
                {
                    'item': record['item'],
                    'query': record['query'],
                }
            )

        return grouped

    def write_files(self):
        grouped = self.group_records()

        payload = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'total': len(self.records),
            'sections': grouped,
        }
        with open(self.json_path, 'w', encoding='utf-8') as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

        lines = []
        if not grouped:
            lines.append('All tracks were imported successfully.')
        else:
            for section in sorted(grouped.keys()):
                lines.append(f'[{section}]')
                section_reasons = grouped[section]
                for reason in ['not_found', 'search_error']:
                    items = section_reasons.get(reason, [])
                    lines.append(f'{reason}: {len(items)}')
                    for item in items:
                        lines.append(f'- {item["item"]} | query: {item["query"]}')
                lines.append('')

        with open(self.txt_path, 'w', encoding='utf-8') as file:
            file.write('\n'.join(lines).rstrip() + '\n')

    def print_summary(self):
        logger.error('Not imported items:')
        if not self.not_imported:
            logger.info('None')
            return

        for section, items in self.not_imported.items():
            logger.info(f'{section}:')
            for item in items:
                logger.info(item)

    def finalize(self):
        try:
            self.print_summary()
        except Exception:
            logger.exception('Failed to print not imported items.')

        try:
            self.write_files()
        except Exception:
            logger.exception('Failed to write not found tracks files.')
