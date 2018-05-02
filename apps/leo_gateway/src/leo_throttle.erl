%%======================================================================
%%
%% Leo Gateway Large Throttle
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%======================================================================
-module(leo_throttle).

-include("leo_gateway.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(gen_server).

%% APIs
-export([start_link/2,
         stop/1,
         alloc/2, free/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {resources  :: pos_integer(),
                wait_queue :: queue:queue({pid(), pos_integer()})
               }).

%% ===================================================================
%% API functions
%% ===================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
start_link(Id, MaxResource) ->
    gen_server:start_link({local, Id}, ?MODULE, [MaxResource], []).

stop(Id) ->
    gen_server:call(Id, stop, 30000).

%% @doc alloc
%% Try to alloc a specified amount of the resource.
%% Block till enough resources become available.
%% Once the resouce you got is no longer needed, call free/2 with the same amount of the resouce.
alloc(Id, Resource) ->
    gen_server:call(Id, {alloc, Resource}, infinity).

%% @doc free
%% Free a specified amount of the resource.
%% Callers waiting for resources available may be unblocked if enough resources become available.
free(Id, Resource) ->
    gen_server:call(Id, {free, Resource}, infinity).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([MaxResource]) ->
    {ok, #state{resources = MaxResource,
                wait_queue = queue:new()
               }}.

handle_call(stop,_From, State) ->
    {stop, normal, ok, State};

handle_call({alloc, Resource},_From, #state{resources = CurResources} = State) when CurResources >= Resource ->
    {reply, ok, State#state{resources = CurResources - Resource}};
handle_call({alloc, Resource}, From, #state{wait_queue = WaitQ} = State) ->
    NewWaitQ = queue:in({From, Resource}, WaitQ),
    {noreply, State#state{wait_queue = NewWaitQ}};
handle_call({free, Resource},_From, #state{resources = CurResources, wait_queue = WaitQ} = State) ->
    case queue:out(WaitQ) of
        {empty, _} ->
            {reply, ok, State#state{resources = CurResources + Resource}};
        {{value, {Pid, ResourceNeeded}}, WaitQ2} ->
            case ResourceNeeded =< (CurResources + Resource) of
                true ->
                    gen_server:reply(Pid, ok),
                    {reply, ok, State#state{resources = CurResources + Resource - ResourceNeeded,
                                            wait_queue = WaitQ2}};
                false ->
                    {reply, ok, State#state{resources = CurResources + Resource}}
            end
    end;
handle_call(_Unknown,_From, State) ->
    %% Just ignore
    {reply, ok, State}.

%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
handle_cast(_, State) ->
    {noreply, State}.


%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.


%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.


%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
