# concurrency-control-protocol

## Description

This repository contains a simulations of database concurrency control written in Python to fulfill the course "Database Management" at Bandung Institute of Technology. 

## Requirements

- Python 3

## How to run

1. Run the desired simulation with this command from the root directory of this project

   ```python
   # two phase locking
   python ./src/two_phase_locking.py
   ```
   or
   ```python
   # Optimistic Concurrency Control 
   python ./src/two_phase_locking.py
   ```
   

3. Then, input the transactions in this format

   ```
   <operation type><transaction id>optional:(<item data id>)
   ```
   each transaction is separated by a semicolon (except the end) 

   e.g.

   ```
   R1(A);R2(A);W1(A);R2(B);W2(A);W1(B);C1;C2
   ```
## Contributor
1. Arleen Chrysantha Gunardi	|	13521059
2. Yobel Dean Christopher		|	13521067
3. Saddam Annais Shaquille	|	13521121
4. Reza Pahlevi Ubaidillah	|	13521165
