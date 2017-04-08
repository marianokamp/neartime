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

Dealing with process events and incomplete processes can be done while writing the process events
or when querying the process events. 

We will first look at the approach to maintain the process view when writing, subsequently when querying.

### Maintaining process view while writing - Step by step

Let's go back to the broken sequence and consider this scenario step by step.

![Broken Sequence](assets/broken_sequence.png)

Event A is emitted from an operational system. It will be stored in table Events. 
As it has no predecessor it must be a root event and a new process container is to be created.
To make everything accessible using primary key access only, 
a lookup table, secondary index processes-index is maintained as well.

---

**Events**

|ID | Predecessor ID|Time|Payload|
|---|---------------|----|-------|
|A|- (root)|t1|Ref-A-99|

The Payload from above contains arbitrary information from a specific event that may be used in a status visualisation
or for cross-referencing/lookup. A customer number or order ID or step description would be example payloads. 
It can also be used to lookup an event (and associated process).


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

|ID | Predecessor ID|Time|Payload|
|---|---------------|----|
|A|- (root)|t1|Ref-A-99|
|**B**|**A**|**t2**|**Ref-B-88**|

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

|ID | Predecessor ID|Time|Payload|
|---|---------------|----|-------|
|A|- (root)|t1|Ref-A-99|
|B|A|t2|Ref-B-88|
|**D**|**C**|**t3**|**Ref-D-66**|

**Processes-Index**

|ID | Process ID|
|---|--------|
|A|P1|
|B|P1|

Looking up the predecessor event C fails. So the only sensible thing to do is to create a new process, 
even though it is clear that the current event does point to a predecessor and therefore cannot be the root of a sequence. 

Also the processes index must be maintained as well. 

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
To avoid unspecific and wide sweeping cleanup activities later on we take note of the missing parent.

**Orphaned-Events**

|Missing Parent ID | Orphaned Event ID|
|---|--------|
|**C**|**D**|

---

Processing event E is now more of the same, except that its predecessor relationship is now pointing to a disconnected predecessor.

This, however, is transparent to event E and so no special case logic is needed here.

**Events**

|ID | Predecessor ID|Time|Payload|
|---|---------------|----|-------|
|A|- (root)|t1|Ref-A-99|
|B|A|t2|Ref-B-88|
|D|C|t3|Ref-D-66
|**E**|**C**|**t4**|**Ref-E-55|


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

We now have two disconnected sequences as illustrated earlier already, and here repeated for convenience: 

![Broken Sequence](assets/broken_sequence.png)

---

Finally, to much applause, event C arrives and is processed.

Writing the event itself happens as before:

**Events**

|ID | Predecessor ID|Time|Payload|
|---|---------------|----|-------|
|A|- (root)|t1|Ref-A-99|
|B|A|t2|Ref-B-88|
|D|C|t3|Ref-D-66|
|E|D|t4|Ref-E-55|
|**C**|**B**|**t5**|Ref-C-77|

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


### Testcases

[Test Suite](https://github.com/marianokamp/neartime/blob/master/src/test/scala/neartime/NeartimeTestSuite.scala)

### Maintaining process view while querying - Step by step

### Contrasting both approaches

Let's first look at shared characteristics of both approaches. It is necessary in both cases
to have constant time access or better (O(1) or O(log(n))), 
given that events are usually counted in the hundred thousands or hundred millions. The reconciliation may only
impact a small number of events and even for those the majority may be reconciled within in seconds, but it may necessary to 
consider if some events may only be processed after days or weeks or may get lost completely.

Obviously the write oriented approach makes querying easier and vice versa. 
The overall complexity of the query oriented approach is smaller, but given that it shifts the harder part of the 
logic to the query side it is necessary to consider the tools and skills available on the query side. 


### Storing the events in HBase
Given the distributed nature of the event emissions and anticipated large volumes of events, 
the data would be transmitted through Kafka and stored in HBase.

### Concurrency issues
#### Report isolation

### Access complexity

### Candidates for alternative approaches

- RDBMS
- Global Sequences
- Global fully qualified composite keys

Using some global ID in the payload to associate events is often not straight forward. For one all systems that are part of the processing must be able
to emit such global IDs and you may not have that much control over them. 
But also, non-trivial processes may cross boundaries, that are not captured by a single ID. 

_In our Telco example the customer creation and overall order management would happen in a [BSS](https://en.wikipedia.org/wiki/Business_support_system) System, 
that deals with commercial orders, contracts, products and customers. The actual network provisioning is happening on the [OSS](https://en.wikipedia.org/wiki/Operations_support_system) side however,
where the entities are network elements and the commercial entities are not known._

_Also the granularity of a global ID may not match our needs. The initial steps of a process may deal with a bulk order submitted by an external 
sales organisation. We want to track the reception and unpacking of those bulk orders, 
but at the time the underlying order numbers are not known and the state needs to applied to many events as a predecessor._

### Alternative approaches that work

