# Magic Boar Kafka Connector

So, you decided to hop onto the Magic Boar Kafka Connector train, eh? Buckle up, buttercup. Here's the lowdown:

---

## What Is This Thing?

Some might call it a _“Kafka Connector”_, but I prefer “that wacky script that talks to Kafka clusters and doesn't judge me for my questionable life choices.” It's basically a PyQt GUI that helps you manage Kafka topics, produce messages, consume them, and be so sassy while doing it.

---

## Installation & Binaries

### 1. Running from Source

1. Clone this repo with your favorite method:
   git clone https://github.com/greqone/MagicBoarKafkaConnector.git
2. Install dependencies:
   pip install -r requirements.txt
3. Launch the script:
   python main_gui_v2.py

Then watch it do its Kafka magic.

### 2. Portable EXE
If you’re too cool to deal with dependencies, there's a portable EXE inside the dist/ folder (once built). Just grab the Magic Boar Kafka Connector.exe file and double-click. No installation required! (Your antivirus might grumble a bit, but hey, that's showbiz, baby.)

### 3. Installer
If you’re more of a “click Next > Next > Finish” type, there's an installer in Output/. It's conveniently named Magic Boar Kafka Connector-Setup.exe. Run it, let it do its thing, and soon you'll be politely bossing Kafka around.

# Files & Folders
main_gui_v2.py – The main code for all your Kafka conquests.
resources/ – Holds logo.ico & background.png or whatever fancy assets you want.
dist/ – Houses the portable EXE once PyInstaller does its dance.
Output/ – Holds the snazzy installer if you're feeling official.

# Requirements
Python 3.12+ (Because we like to live on the cutting edge)
PyQt5
kafka-python-ng

More details in requirements.txt.

# Running from Source (Detailed)
Create a virtual environment:
   python -m venv .venv
   source .venv/bin/activate  # or .venv\Scripts\activate on Windows
Install dependencies:
   pip install --upgrade pip
   pip install -r requirements.txt
Run it:
   python main_gui_v2.py

Enjoy your Kafka wizardry.


## Why "Magic Boar"?
Because normal boars are so mainstream. Ours is magical (obviously).

## Contributing
Fork, PR, maybe some incantations—whatever floats your boat.

If you're reading this, you’re already 93% cooler than the average developer.

## License
Magic Boar License

This software is free for personal and commercial use if your organization has fewer than 25 employees. For usage within a larger company, please reach out for a custom licensing arrangement. We also allow private use—go wild, do your boar-y magic.

If anything breaks, I blame you. Just kidding… sort of.

Enjoy your Kafka adventures!