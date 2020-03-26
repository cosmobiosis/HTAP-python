The whole course project is based on implementation of a databse model from scratch.
The model is invented by Professor Mohammad Sadoghi and presented in paper *L-Store: A Real-time OLTP and OLAP System*
https://www.researchgate.net/publication/324150481_L-Store_A_Real-time_OLTP_and_OLAP_System

The paper can also be downloaded within this repo (filename: l-store.pdf)

Testers exposes basic usage of API for lstore database. 

API exposed is within the top level Query abstraction layer while I decided to use five abstraction layers in total.

These five abstraction layers are: Query->Table(manage logic and index)->Cache(Manage in-memory Pages)->Disk Helper(Manage interaction with durable in-disk files)

For any concurrent transaction programs, please install readerwriterlock

python3 -m pip install -U readerwriterlock

The package website is:
https://pypi.org/project/readerwriterlock/
