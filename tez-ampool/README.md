tez-ampool
===========

tez-ampool is a standalone service that manages a pool of MapReduce AMs and supports YARN's ClientRMProtocol
to act as a proxy for submitting a job to one of the pre-launched MR AMs. The pre-launched MR AMs can in turn
be configured to come up and keep available a number of 'warm' containers to allow jobs execution to start
quickly.
