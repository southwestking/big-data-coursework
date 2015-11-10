#!/bin/bash
rm -rf out/01;
hadoop-moonshot jar dist/OlympicTweets.jar OlympicTweets /data/olympictweets coursework/out;
hadoop-moonshot fs -copyToLocal coursework/out out/01;
