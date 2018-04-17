# Pancarte DB

## How it works

### What does it stores?

From the monitors:

* Raw waveforms (high frequency data, does not stores timestamps for each value, needs constant frequency data)
* Numerics (low frequency data, stores a value with a timestamp)

From other sources:

* Timestamp-based annotations
* Timerange-based annotations
* Metadata such as bed_id, signal_type...


### What can it do?

Writes:

* [ ] Write waveforms, numerics
* [ ] Write annotations

Read (you can combine these options):

* Get data from date A to date B
* Get data where record_length >= 2hours
* Get data where bed_id=X, signal_type=ECG
* Get data where there are arythmia annotations


## Running it

```
python3 server.py hostname port
```