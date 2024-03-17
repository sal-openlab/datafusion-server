---
title: What's Session Context
weight: 10
---

{{< toc >}}

## Session Context

A session context is a collection unit of tables that is maintained for a certain period. Each session context is accessed by an ID, and they are completely isolated from one another.

## Session and Session-less

In contrast to the session-less queries demonstrated in [Basic Queries]({{< ref "/basic-query" >}}), which handle everything from data source definition to query execution and response in a single request, the session context allows for the addition, refresh (reloading), and deletion of tables loaded from the data source at any time. This can reduce the overhead of loading from the data source and expanding into the Arrow in-memory buffer when accessing the same table repeatedly.
