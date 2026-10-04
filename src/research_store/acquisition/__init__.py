"""Publisher API acquisition: the only layer that may open a network socket.

An acquisition module downloads responses into ``downloads/<dataset>/`` and
writes a selection manifest in the format its ingestion module declares. It
never touches the catalogue or the warehouse; ingestion of the manifest is a
separate, offline step.
"""
