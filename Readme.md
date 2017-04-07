## Motivation
### Monitoring of event processing

For a given process multiple events may be produced and each event points to its predecessors, except for the initial event, which is the root of the event sequence:

![Process and Event](assets/process_event.png)

A sequence of process events may look like this:

![Intact Sequence](assets/intact_sequence.png)

A process progresses from event A to B to C to D to E. Every new process event knows about its predecessor. In this simplified version branching is not considered, neither are final states.

_If you need to have a mental image of a real world process you may think of a Telco selling and provisioning a fixed line product bundle (TV, Telephony, Internet): To provision the products the customer has to be created, hardware has to be sent to the install address and the networks have to be provisioned ..._

### Out of sequence event processing
Ingesting process events often times in not straight forward however. In a distributed world events may be produced in sequence, but the event emission and subsequent ingestion may be out of sequence.

This can be due to several reasons. One reason for this may be **slow event producers**. 
Keeping in mind that the actual focus of operational systems is to progress the process and not to report on it,
the actual process execution may have a higher priority and events are emitted with less priority. For example the actual work carried out could be done in milliseconds and the notification of the next applications may happen almost instantly, but emitting the process events may only happen in a scheduled batch every 5 minutes.

Another reason for out of sequence event ingestion may be that events use different routes to reach the processing engine or that they are ingested in parallel and that they are in a queue that is busier than the others. 

And sometimes events may **disappear** completely or are delayed for an extended amount of time, say hours or days.

Continuing the picture from above, the sequence of events would be broken if event C would be delivered after events D and E:

![Broken Sequence](assets/broken_sequence.png)

The above illustrates the broken sequence when event C has not been received yet.

### Benefits of analyzing incomplete event sequences

Because of the missing link between the events B and D, the process as whole, cannot be understood as of now.
However there is still information that can be gained from the two disconnected sequences. 

**Monitoring** the overall systems performance is still possible. 
For example the transition times between A and B can be reported on as can the transition between D and E.

The events shown here are simplified. 
They would certainly contain some payload data. 
Using this information events could be searched for and the retrieved sub sequence
of the process can be used to show an **approximate status** or a **partial status history**.

_Consider the case of an order status with a Telco. 
Knowing that the service technician visit to the customer's house is scheduled for tomorrow,
say Event E, is still helpful information, even though the status can't tell us when the order was received._

### Step by step

Let's go back to the broken sequence and consider this scenario step by step.

![Broken Sequence](assets/broken_sequence.png)

Event A is emitted from an operational system. It will be stored in table Events. 
As it has no predecessor it must be a root event and a new process container is to be created.
To make everything accessible using primary key access only, 
a lookup table, secondary index processes-index is maintained as well.

---

**Events**

|ID | Predecessor ID|Time|
|---|---------------|----|
|A|- (root)|t1|

**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|

**Processes**

|ID | Event IDs|
|---|--------|
|P1|A|


---

The story repeats for Event B, except that event B is not root and therefore has a predecessor: Event A. 
Also, using its predecessor's event ID the containing process is looked up and its own ID is merged into process P1.


**Events**

|ID | Predecessor ID|Time|
|---|---------------|----|
|A|- (root)|t1|
|**B**|**A**|**t2**|

**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|
|**B**|**P1**|

**Processes**

|ID | Event IDs|
|---|--------|
|P1|A, **B**|

---

Now continuing with event D, skipping event C for the time being, things become more interesting.

**Events**

|ID | Predecessor ID|Time|
|---|---------------|----|
|A|- (root)|t1|
|B|A|t2|
|**D**|**C**|**t3**|

**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|
|B|P1|

Looking up the predecessor event C fails. So the only sensible thing to do is to create a new process, 
even though it is clear that the current event does point to a predecessor and therefore cannot be the root of a sequence. Also the processes index must be maintained as well. 

**Processes**

|ID | Event IDs|
|---|--------|
|P1|A, B|
|**P2**|**D**|

**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|
|B|P1|
|**D**|**P2**|

But because it is clear that the current event is pointing to a predecessor or parent, that does not exist, the current event is orphaned. 
To avoid unspecific and wide sweeping cleanup activities afterwards we take note of the missing parent.

**Orphaned-Events**

|Missing Parent ID | Orphaned Event ID|
|---|--------|
|**C**|**D**|

---

Processing event E is now more of the same, except that its predecessor relationship is now pointing to a disconnected predecessor,
but this is transparent to event E.

**Events**

|ID | Predecessor ID|Time|
|---|---------------|----|
|A|- (root)|t1|
|B|A|t2|
|D|C|t3|
|**E**|**C**|**t4**|


**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|
|B|P1|
|D|P2|
|**E**|**P2**|

**Processes**

|ID | Event IDs|
|---|--------|
|P1|A, B|
|P2|D, **E**|

**Orphaned-Events**

|Missing Parent ID | Orphaned Event ID|
|---|--------|
|C|D|

No change to the orphaned events.

We now have two disconnected sequences as illustrated earlier already: 

![Broken Sequence](assets/broken_sequence.png)

---

Finally, to much applause, event C arrives and is processed.

Writing the event itself happens as before:

**Events**

|ID | Predecessor ID|Time|
|---|---------------|----|
|A|- (root)|t1|
|B|A|t2|
|D|C|t3|
|E|D|t4|
|**C**|**B**|**t5**|

Except after each successful write of a new event the orphaned events are checked and 
hence here it is detected that we were waiting for event C.

**Orphaned-Events**

|Missing Parent ID | Orphaned Event ID|
|---|--------|
|**~~C~~**|**~~D~~**|

The marker of the orphan has to be deleted, the processes merged and the references updated.

**Processes**

|ID | Event IDs|
|---|--------|
|P1|A, B, **D**, **E**|
|**~~P2~~**|**~~D~~**, **~~E~~**|


**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|
|B|P1|
|**C**|**P1**|
|D|**~~P2~~** **P1**|
|E|**~~P2~~** **P1**|


As a result the sequence now looks the same as if it would have never been broken.

![Intact Sequence](assets/intact_sequence.png)



### Storing the events in HBase
Given the distributed nature of the event emissions and anticipated large volumes of events, 
the data would be transmitted through Kafka and stored in HBase.

### Concurrency issues
#### Report isolation

### Access complexity

### Alternative approaches




