/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * This is an ANTLR4 grammar for Kafka connector mappings (see MappingInspector).
*/

grammar Mapping;

mapping
    : mappedEntry  ( ',' mappedEntry  )* EOF
    ;

mappedEntry
    : column '=' field
    ;

field
    : UNQUOTED_STRING (UNQUOTED_STRING | '.')*
    | QUOTED_STRING
    | function
    ;

column
    : UNQUOTED_STRING
    | QUOTED_STRING
    ;

function
    : functionName '(' ')'
    | functionName '(' functionArgs ')'
    ;

functionName
    : ( identifier '.' )? identifier
    ;

functionArgs
    :  identifier ( ',' identifier )*
    ;

identifier
    : UNQUOTED_STRING
    | QUOTED_STRING
    ;

fragment ALPHANUMERIC
    : ( 'A'..'Z' | 'a'..'z' | '0'..'9' | '_' )
    ;

UNQUOTED_STRING
    : ALPHANUMERIC ( ALPHANUMERIC )*
    ;

QUOTED_STRING
    : '"' ( ~'"' | '"' '"' )+ '"'
    ;

WS
    : ( ' ' | '\t' | '\n' | '\r' )+ -> channel(HIDDEN)
    ;

