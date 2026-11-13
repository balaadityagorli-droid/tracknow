/* Tiny Lua runner for the dev Redis stand-in. Declares the few Lua 5.4 symbols it needs, so no headers are required.
   Build: see build.sh */
#include <stdio.h>
typedef struct lua_State lua_State;
lua_State *luaL_newstate(void);
void luaL_openlibs(lua_State *);
int luaL_loadfilex(lua_State *, const char *, const char *);
int lua_pcallk(lua_State *, int, int, int, long, void *);
const char *lua_tolstring(lua_State *, int, unsigned long *);
int main(int argc, char **argv) {
  lua_State *L = luaL_newstate();
  luaL_openlibs(L);
  if (argc < 2 || luaL_loadfilex(L, argv[1], 0) || lua_pcallk(L, 0, 0, 0, 0, 0)) {
    fprintf(stderr, "lua-host: %s\n", lua_tolstring(L, -1, 0));
    return 1;
  }
  return 0;
}
