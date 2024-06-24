-module(life_fs_ffi).

-export([execute_function/1, dynamic_to_subject/1]).

execute_function(Fun) ->
    Fun().

dynamic_to_subject(Dynamic) ->
    Dynamic.
