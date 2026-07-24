
file (GLOB HEADER_FILES *.hpp)

# The proxy example executable.
init_target (websocket_proxy)
build_executable (${TARGET_NAME} websocket_proxy.cpp ${HEADER_FILES})
link_boost ()
final_target ()
set_target_properties(${TARGET_NAME} PROPERTIES FOLDER "examples")

# End-to-end test: a pingpong upstream server, the proxy, and multiple clients,
# verifying message relay and subprotocol negotiation through the proxy. Runs
# under ctest.
init_target (websocket_proxy_test)
build_test (${TARGET_NAME} proxy_test.cpp ${HEADER_FILES})
link_boost ()
final_target ()
set_target_properties(${TARGET_NAME} PROPERTIES FOLDER "examples")
