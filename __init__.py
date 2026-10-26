"""Manual OpenCode history sync. No background job or model calls."""

def setup(parser):
    parser.add_argument('--dry-run', action='store_true', help='Show new/changed chats without writing history or sync state')
    parser.add_argument('--directory', help='Only sync this exact working directory (e.g. --directory .)')
    parser.add_argument('--session', help='Only sync this literal OpenCode session ID')
    parser.add_argument('--source', help='Read another OpenCode SQLite database instead of the XDG default')
    parser.add_argument('--json', action='store_true', help='Print a machine-readable result')


def run(args):
    from .client import main
    return main(args)


def register(ctx):
    ctx.register_cli_command('opencode-sync', 'Import new and refresh changed OpenCode sessions', setup, run)
