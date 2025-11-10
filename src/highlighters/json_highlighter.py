"""JSON syntax highlighter using QRegularExpression (Qt6 compatible)."""
from PyQt5.QtCore import QRegularExpression
from PyQt5.QtGui import QSyntaxHighlighter, QTextCharFormat, QColor


class JsonHighlighter(QSyntaxHighlighter):
    """Syntax highlighter for JSON text using modern Qt API."""

    def __init__(self, document):
        """
        Initialize JSON highlighter.

        Args:
            document: QTextDocument to highlight
        """
        super().__init__(document)
        self.rules = []

        # String format (keys and values)
        string_format = QTextCharFormat()
        string_format.setForeground(QColor('green'))

        # JSON keys (strings followed by colon)
        self.rules.append((QRegularExpression(r'"[^"]*"(?=\s*:)'), string_format))

        # JSON string values (strings after colon)
        self.rules.append((QRegularExpression(r'(?<=:\s*)"[^"]*"'), string_format))

        # Number format
        number_format = QTextCharFormat()
        number_format.setForeground(QColor('magenta'))
        self.rules.append((QRegularExpression(r'\b\d+\.?\d*\b'), number_format))

        # Boolean and null format
        bool_format = QTextCharFormat()
        bool_format.setForeground(QColor('red'))
        self.rules.append((QRegularExpression(r'\b(true|false|null)\b'), bool_format))

        # Brace format
        brace_format = QTextCharFormat()
        brace_format.setForeground(QColor('blue'))
        self.rules.append((QRegularExpression(r'[\{\}\[\]]'), brace_format))

        # Colon format
        colon_format = QTextCharFormat()
        colon_format.setForeground(QColor('black'))
        self.rules.append((QRegularExpression(r':'), colon_format))

        # Comma format
        comma_format = QTextCharFormat()
        comma_format.setForeground(QColor('black'))
        self.rules.append((QRegularExpression(r','), comma_format))

    def highlightBlock(self, text):
        """
        Highlight a block of text.

        Args:
            text: Text to highlight
        """
        for pattern, fmt in self.rules:
            match_iterator = pattern.globalMatch(text)
            while match_iterator.hasNext():
                match = match_iterator.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), fmt)
