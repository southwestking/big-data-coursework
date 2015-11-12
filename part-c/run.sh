#!/bin/bash
rm -rf out/02;
hadoop-moonshot jar dist/OlympicTweets.jar OlympicTweets /data/olympictweets coursework/out;
hadoop-moonshot fs -copyToLocal coursework/out out/01;
