MAIN_STYLE = """
/* Global */
QWidget {
    background-color: #121212;
    color: #E0E0E0;
    font-family: "Segoe UI", "San Francisco", "Helvetica Neue", Arial, sans-serif;
    font-size: 13px;
}

/* Headers / Panels */
#LeftPanel, #CenterPanel, #RightPanel {
    background-color: #1E1E1E;
    border: 1px solid #333333;
    border-radius: 8px; 
    margin: 4px;
}
#Header {
    background-color: #1A1A1A;
    border-bottom: 2px solid #2D2D2D;
}
QLabel#AppTitle {
    font-size: 18px;
    font-weight: bold;
    color: #FFFFFF;
    padding: 10px;
}

/* Inputs */
QLineEdit, QComboBox {
    background-color: #2D2D2D;
    border: 1px solid #404040;
    border-radius: 4px;
    padding: 6px;
    color: #FFFFFF;
}
QLineEdit:focus, QComboBox:focus {
    border: 1px solid #5A5A5A;
}

/* Buttons */
QPushButton {
    background-color: #2A2A2A;
    border: 1px solid #444444;
    border-radius: 4px;
    padding: 8px 16px;
    color: #FFFFFF;
    font-weight: bold;
}
QPushButton:hover {
    background-color: #3A3A3A;
    border: 1px solid #555555;
}
QPushButton:pressed {
    background-color: #1A1A1A;
}
QPushButton:disabled {
    background-color: #1A1A1A;
    color: #555555;
    border: 1px solid #222222;
}

QPushButton#RunButton {
    background-color: #007ACC;
    border: 1px solid #005C99;
}
QPushButton#RunButton:hover {
    background-color: #0088E0;
}
QPushButton#StopButton {
    background-color: #8C2222;
    border: 1px solid #661111;
}
QPushButton#StopButton:hover {
    background-color: #AA2222;
}

/* Text / Logs */
QTextEdit, QListWidget {
    background-color: #0C0C0C;
    border: 1px solid #333333;
    border-radius: 4px;
    color: #D4D4D4;
    font-family: Consolas, "Courier New", monospace;
    font-size: 12px;
}

/* Scrollbars */
QScrollBar:vertical {
    border: none;
    background: #1E1E1E;
    width: 10px;
    margin: 0;
}
QScrollBar::handle:vertical {
    background: #444444;
    min-height: 20px;
    border-radius: 5px;
}
QScrollBar::handle:vertical:hover {
    background: #555555;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    border: none;
    background: none;
}
"""
