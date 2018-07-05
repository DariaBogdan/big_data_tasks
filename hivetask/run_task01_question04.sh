#!/bin/bash
beeline -u jdbc:hive2://localhost:10000/default -n root -f task01_question04.hql
