lexer grammar WdlV2Lexer;

channels { COMMENTS }

// Comments
LINE_COMMENT: '#' ~[\r\n]* -> channel(COMMENTS);

// Keywords
VERSION: 'version' -> pushMode(Version);
IMPORT: 'import';
WORKFLOW: 'workflow';
TASK: 'task';
STRUCT: 'struct';
SCATTER: 'scatter';
CALL: 'call';
IF: 'if';
THEN: 'then';
ELSE: 'else';
ALIAS: 'alias';
AS: 'as';
In: 'in';
INPUT: 'input';
OUTPUT: 'output';
PARAMETERMETA: 'parameter_meta';
META: 'meta';
HINTS: 'hints';
RUNTIME: 'runtime';
RUNTIMECPU: 'cpu';
RUNTIMECONTAINER: 'container';
RUNTIMEMEMORY: 'memory';
RUNTIMEGPU: 'gpu';
RUNTIMEDISKS: 'disks';
RUNTIMEMAXRETRIES: 'maxRetries';
RUNTIMERETURNCODES: 'returnCodes';
BOOLEAN: 'Boolean';
INT: 'Int';
FLOAT: 'Float';
STRING: 'String';
FILE: 'File';
DIRECTORY: 'Directory';
ARRAY: 'Array';
MAP: 'Map';
PAIR: 'Pair';
AFTER: 'after';
COMMAND: 'command'-> mode(Command);

// Primitive Literals
NONELITERAL: 'None';
IntLiteral
	: Digits
	| SignedDigits
	;
FloatLiteral
	: SignedFloatFragment
	| FloatFragment
	;
BoolLiteral
	: 'true'
	| 'false'
	;

// Symbols
LPAREN: '(';
RPAREN: ')';
LBRACE: '{' -> pushMode(DEFAULT_MODE);
RBRACE: '}' -> popMode;
LBRACK: '[';
RBRACK: ']';
ESC: '\\';
COLON: ':';
LT: '<';
GT: '>';
GTE: '>=';
LTE: '<=';
EQUALITY: '==';
NOTEQUAL: '!=';
EQUAL: '=';
AND: '&&';
OR: '||';
OPTIONAL: '?';
STAR: '*';
PLUS: '+';
MINUS: '-';
DOLLAR: '$';
COMMA: ',';
SEMI: ';';
DOT: '.';
NOT: '!';
TILDE: '~';
DIVIDE: '/';
MOD: '%';
SQUOTE: '\'' -> pushMode(SquoteInterpolatedString);
DQUOTE: '"' -> pushMode(DquoteInterpolatedString);

WHITESPACE
	: [ \t\r\n]+ -> channel(HIDDEN)
	;

Identifier: CompleteIdentifier;

mode SquoteInterpolatedString;

SQuoteEscapedChar: '\\' . -> type(StringPart);
SQuoteDollarString: '$'  -> type(StringPart);
SQuoteTildeString: '~' -> type(StringPart);
SQuoteCurlyString: '{' -> type(StringPart);
SQuoteCommandStart: ('${' | '~{' ) -> pushMode(DEFAULT_MODE) , type(StringCommandStart);
SQuoteUnicodeEscape: '\\u' (HexDigit (HexDigit (HexDigit HexDigit?)?)?)? -> type(StringPart);
EndSquote: '\'' ->  popMode, type(SQUOTE);
StringPart: ~[$~{\r\n']+;

mode DquoteInterpolatedString;

DQuoteEscapedChar: '\\' . -> type(StringPart);
DQuoteTildeString: '~' -> type(StringPart);
DQuoteDollarString: '$' -> type(StringPart);
DQUoteCurlString: '{' -> type(StringPart);
DQuoteCommandStart: ('${' | '~{' ) -> pushMode(DEFAULT_MODE), type(StringCommandStart);
DQuoteUnicodeEscape: '\\u' (HexDigit (HexDigit (HexDigit HexDigit?)?)?) -> type(StringPart);
EndDQuote: '"' ->  popMode, type(DQUOTE);
DQuoteStringPart: ~[$~{\r\n"]+ -> type(StringPart);

mode Command;

BeginWhitespace: [ \t\r\n]* -> channel(HIDDEN);
BeginHereDoc: '<<<' -> mode(HereDocCommand);
BeginLBrace: '{' -> mode(CurlyCommand);

mode HereDocCommand;

HereDocUnicodeEscape: '\\u' (HexDigit (HexDigit (HexDigit HexDigit?)?)?)?;
HereDocEscapedChar: '\\' . -> type(CommandStringPart);
HereDocTildeString: '~' -> type(CommandStringPart);
HereDocCurlyString: '{' -> type(CommandStringPart);
HereDocCurlyStringCommand: '~{' -> pushMode(DEFAULT_MODE), type(StringCommandStart);
HereDocEscapedEnd: '\\>>>' -> type(CommandStringPart);
EndHereDocCommand: '>>>' -> mode(DEFAULT_MODE), type(EndCommand);
HereDocEscape: ( '>' | '>>' | '>>>>' '>'*) -> type(CommandStringPart);
HereDocStringPart: ~[~{>]+ -> type(CommandStringPart);

mode CurlyCommand;

CommandEscapedChar: '\\' . -> type(CommandStringPart);
CommandUnicodeEscape: '\\u' (HexDigit (HexDigit (HexDigit HexDigit?)?)?)?;
CommandTildeString: '~'  -> type(CommandStringPart);
CommandDollarString: '$' -> type(CommandStringPart);
CommandCurlyString: '{' -> type(CommandStringPart);
StringCommandStart:  ('${' | '~{' ) -> pushMode(DEFAULT_MODE);
EndCommand: '}' -> mode(DEFAULT_MODE);
CommandStringPart: ~[$~{}]+;

mode Version;

VersionWhitespace: [ \t]+ -> channel(HIDDEN);
ReleaseVersion: [a-zA-Z0-9.-]+ -> popMode;

// Fragments

fragment CompleteIdentifier
	: IdentifierStart IdentifierFollow*
	;

fragment IdentifierStart
	: [a-zA-Z]
	;

fragment IdentifierFollow
	: [a-zA-Z0-9_]+
	;

fragment EscapeSequence
    : '\\' [btnfr"'\\]
    | '\\' ([0-3]? [0-7])? [0-7]
    | '\\' UnicodeEsc
    ;

fragment UnicodeEsc
   : 'u' (HexDigit (HexDigit (HexDigit HexDigit?)?)?)?
   ;

fragment HexDigit
   : [0-9a-fA-F]
   ;

fragment Digit
	: [0-9]
	;

fragment Digits
	: Digit+
	;

fragment Decimals
	: Digits '.' Digits? | '.' Digits
	;

fragment SignedDigits
	: ('+' | '-' ) Digits
	;

fragment FloatFragment
	: Digits EXP?
	| Decimals EXP?
	;

fragment SignedFloatFragment
	: ('+' |'e') FloatFragment
	;

fragment EXP
	: ('e'|'E') SignedDigits
	;
