!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
<head><title>Apache Tez Project Bylaws</title></head>

# Apache Tez Project Bylaws

This document defines the bylaws under which the Apache Tez project operates. It defines the roles and responsibilities of the project, who may vote, how voting works, how conflicts are resolved, etc.

Tez is a project of the [Apache Software Foundation]. The Foundation holds the copyright on Apache code including the code in the Tez codebase. The [Foundation FAQ] explains the operation and background of the foundation.

Tez is typical of Apache projects in that it operates under a set of principles, known collectively as the "Apache Way". If you are new to Apache development, please refer to the [Incubator project] for more information on how Apache projects operate.

##Roles and Responsibilities

Apache projects define a [set of roles] with associated rights and responsibilities. These roles govern what tasks an individual may perform within the project. The roles are defined in the following sections:

###Users

The most important participants in the project are people who use our software. The majority of our developers start out as users and guide their development efforts from the user's perspective.

Users contribute to the Apache projects by providing feedback to developers in the form of bug reports and feature suggestions. As well, users participate in the Apache community by helping other users on mailing lists and user support forums.

###Developers

All of the volunteers who are contributing time, code, documentation, or resources to the Tez Project. A developer that makes sustained, welcome contributions to the project may be invited to become a Committer, though the exact timing of such invitations depends on many factors.

###Committers

The project's Committers are responsible for the project's technical management. All committers have write access to the project's source repositories. Committers may cast binding votes on any technical discussion regarding the project.

Committer access is by invitation only and must be approved by lazy consensus of the active PMC members. A Committer may request removal of their commit privileges by their own declaration. A committer will be considered "emeritus/inactive" by not contributing in any form to the project for over 1 year. An emeritus committer may request reinstatement of commit access from the PMC. Such reinstatement is subject to lazy consensus of active PMC members.

Commit access can be revoked by a unanimous vote of all the active PMC members (except the committer in question if they are also a PMC member).

All Apache committers are required to have a signed Contributor License Agreement ([CLA]) on file with the Apache Software Foundation. There is a [Committer FAQ] which provides more details on the requirements for Committers

A committer who makes a sustained contribution to the project may be invited to become a member of the PMC. The form of contribution is not limited to code. It can also include code review, helping out users on the mailing lists, documentation, etc.

###Project Management Committee

The Project Management Committee (PMC) for Apache Tez was created by a resolution of the board of the Apache Software Foundation on 16th July, 2014. The PMC is responsible to the board and the ASF for the management and oversight of the Apache Tez codebase. The responsibilities of the PMC include:

   - Deciding what is distributed as products of the Apache Tez project. In particular all releases must be approved by the PMC
   - Maintaining the project's shared resources, including the codebase repository, mailing lists, websites.
   - Speaking on behalf of the project.
   - Resolving license disputes regarding products of the project
   - Nominating new PMC members and committers
   - Maintaining these bylaws and other guidelines of the project

Membership of the PMC is by invitation only and must be approved by a lazy consensus of active PMC members. A PMC member is considered "emeritus/inactive" by not contributing in any form to the project for over one year. An emeritus PMC member may request reinstatement to the PMC. Such reinstatement is subject to lazy consensus of active PMC members. A PMC member may resign their membership from the PMC by their own declaration. Membership of the PMC can be revoked by an unanimous vote of all the active PMC members other than the member in question.

The chair of the PMC is appointed by the ASF board. The chair is an office holder of the Apache Software Foundation (Vice President, Apache Tez) and has primary responsibility to the board for the management of the projects within the scope of the Tez PMC. The chair reports to the board quarterly on developments within the Tez project. The PMC may consider the position of PMC chair annually, and if supported by a successful vote to change the PMC chair, may recommend a new chair to the board. Ultimately, however, it is the board's responsibility who it chooses to appoint as the PMC chair.

##Decision Making

Within the Tez project, different types of decisions require different forms of approval. For example, the previous section describes several decisions which require "lazy consensus" approval. This section defines how voting is performed, the types of approvals, and which types of decision require which type of approval.

###Voting

Decisions regarding the project are made by votes on the primary project development mailing list (dev@tez.apache.org). Where necessary, PMC voting may take place on the private Tez PMC mailing list. Votes are clearly indicated by subject line starting with [VOTE]. Votes may contain multiple items for approval and these should be clearly separated. Voting is carried out by replying to the vote mail. Voting may take four flavours:

|---|---|
| +1 | "Yes," "Agree," or "the action should be performed." In general, this vote also indicates a willingness on the behalf of the voter in "making it happen" |
| +0 | This vote indicates a willingness for the action under consideration to go ahead. The voter, however will not be able to help. |
| -0 | This vote indicates that the voter does not, in general, agree with the proposed action but is not concerned enough to prevent the action going ahead. |
| -1 | This is a negative vote. On issues where consensus is required, this vote counts as a veto. All vetoes must contain an explanation of why the veto is appropriate. Vetoes with no explanation are void. It may also be appropriate for a -1 vote to include an alternative course of action. |

All participants in the Tez project are encouraged to show their agreement with or against a particular action by voting. For technical decisions, only the votes of active committers are binding. Non binding votes are still useful for those with binding votes to understand the perception of an action in the wider Tez community. For PMC decisions, only the votes of PMC members are binding.

Voting can also be applied to changes made to the Tez codebase. These typically take the form of a veto (-1) in reply to the commit message sent when the commit is made.

###Approvals

These are the types of approvals that can be sought. Different actions require different types of approvals:

|---|---|
| Consensus | For this to pass, all voters with binding votes must vote and there can be no binding vetoes (-1). Consensus votes are rarely required due to the impracticality of getting all eligible voters to cast a vote. |
| Lazy Consensus | Lazy consensus requires 3 binding +1 votes and no binding vetoes. |
| Lazy Majority | A lazy majority vote requires 3 binding +1 votes and more binding +1 votes that -1 votes. |
| Lazy Approval | An action with lazy approval requires at least 1 binding +1 vote unless a -1 vote is received, at which time, depending on the type of action, either lazy majority or lazy consensus approval must be obtained. |
| 2/3 Majority | Some actions require a 2/3 majority of active committers or PMC members to pass. Such actions typically affect the foundation of the project (e.g. adopting a new codebase to replace an existing product). The higher threshold is designed to ensure such changes are strongly supported. To pass this vote requires at least 2/3 of binding vote holders to vote +1 |

###Vetoes

A valid, binding veto cannot be overruled. If a veto is cast, it must be accompanied by a valid reason explaining the reasons for the veto. The validity of a veto, if challenged, can be confirmed by anyone who has a binding vote. This does not necessarily signify agreement with the veto - merely that the veto is valid.

If you disagree with a valid veto, you must lobby the person casting the veto to withdraw their veto. If a veto is not withdrawn, the action that has been vetoed must be reversed in a timely manner.

###Actions

This section describes the various actions which are undertaken within the project, the corresponding approval required for that action and those who have binding votes over the action.

| Action | Description | Approval | Binding Votes |
| ------ | ----------- | -------- | ------------- |
| Code Change | A change made to a codebase of the project and committed by a committer. This includes source code, documentation, website content, etc. | Lazy approval and then Lazy consensus. | Active committers |
| Major Feature/Change via a Branch Merge | A major change made to the codebase of the project done via a branch merge. | Lazy consensus | Active committers | 
| Release Plan | Defines the timetable and actions for a release. The plan also nominates a Release Manager. | Lazy majority | Active committers |
| Product Release | When a release of one of the project's products is ready, a vote is required to accept the release as an official release of the project. | Lazy Majority |   Active PMC members |
| Adoption of New Codebase | When the codebase for an existing, released product is to be replaced with an alternative codebase. If such a vote fails to gain approval, the existing code base will continue. This also covers the creation of new sub-projects within the project. | 2/3 majority | Active committers |
| New Committer | When a new committer is proposed for the project | Lazy consensus | Active PMC members |
| New PMC Member | When a committer is proposed for the PMC | Lazy consensus | Active PMC members |
| Committer Removal | When removal of commit privileges is sought. Note: Such actions will also be referred to the ASF board by the PMC chair | Consensus | Active PMC members (excluding the committer in question if a member of the PMC). |
| PMC Member Removal | When removal of a PMC member is sought. Note: Such actions will also be referred to the ASF board by the PMC chair | Consensus | Active PMC members (excluding the member in question). |
| Change to Project By-Laws | When a change is needed to the Project's By-Laws. | Lazy Consensus | Active PMC Members |
| Change the PMC Chair | When the PMC Chair needs to be changed. | Lazy Consensus | Active PMC Members |

###Voting Timeframes

Votes are open for a period of a minimum of 3 days (excluding weekend days) to allow all active voters time to consider the vote. For any votes requiring full consensus or a 2/3 majority, the vote should remain open for a minimum of 1 week. Votes relating to code changes are not subject to a strict timetable but should be made as timely as possible.


[Apache Software Foundation]: http://www.apache.org/foundation/
[Incubator project]: http://incubator.apache.org/
[Foundation FAQ]: http://www.apache.org/foundation/faq.html
[Committer FAQ]: http://www.apache.org/dev/committers.html
[CLA]: http://www.apache.org/licenses/icla.txt
[set of roles]: http://www.apache.org/foundation/how-it-works.html#roles
