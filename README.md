# Pancarte DB

## How it works

### What does it stores?

From the monitors (immutable data):

* Raw waveforms (high frequency data, does not stores timestamps for each value, needs constant frequency data)
* Numerics (low frequency data, stores a value with a timestamp)
* Metadata such as bed_id, signal_type...

From other sources (mutable data):

* Timestamp-based annotations
* Timerange-based annotations


### What can it do?

Writes:

* [ ] Write waveforms, numerics
* [ ] Write annotations

Read (you can combine these options):

* [ ] Get data from date A to date B
* [ ] Get data where record_length >= 2hours
* [ ] Get data where bed_id=X, signal_type=ECG
* [ ] Get data where there are arythmia annotations


### How does it stores data?

There are two types of data:

* Immutable: waveforms and numerics
* Editable/Expandable: metadata and annotations

Immutable data is directly stored in files that are not supposed to be editable.
Editable/Expandable data is stored in an easily queryable store (sqlite).

![alt text](architecture.png)

## Running it

```
python3 api.py hostname port
```