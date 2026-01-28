Lab 4 — Distributed Transactions (2PC / 3PC)
1. Setup

Connect to each EC2 instance.

Create a Python virtual environment and activate it:

sudo apt update
sudo apt install -y python3-pip python3.12-venv
python3 -m venv myenv
source myenv/bin/activate


Install dependencies:

pip install flask requests

2. Start Participant Nodes

Participant B (port 8001):

source myenv/bin/activate
python3 participant.py --id B --port 8001


Participant D (port 8002):

source myenv/bin/activate
python3 participant.py --id D --port 8002

3. Start Coordinator Node
source myenv/bin/activate
python3 coordinator.py --id C --port 8000 --participants B:54.204.131.177:8001,D:98.84.112.143:8002

4. Run Transactions (Client)

2PC transaction:

python3 client.py transfer TX123 x -10 --protocol 2PC


3PC transaction:

python3 client.py transfer TX_3PC_SUCCESS mode 3phase --protocol 3PC

Notes

Make sure participants are running before the coordinator.

Use private IPs for EC2 communication.

Virtual environment must be activated on each node before running scripts.
