#!/bin/sh

SHA1=`git describe --tags`
echo '
package com.pingcap.tikv;
public final class TiVersion { public final static String CommitVersion = "'${SHA1}'"; }' > src/main/java/com/pingcap/tikv/TiVersion.java
