ERS
===

Entity Registry System

Purpose
=======

TBW

Installation
============

On archlinux starting from a fresh install

pacman -S python2 python2-rdflib python2-gobject2 python2-dbus dbus-glib avahi couchdb

systemctl start couchdb
systemctl restart dbus
systemctl start avahi-daemon

systemctl enable couchdb
systemctl enable avahi-daemon

Add this to the /etc/pacman.conf , it facilitates installing AUR packages
[archlinuxfr]
SigLevel = Never
Server = http://repo.archlinux.fr/$arch

Install Yaourt
pacman -Sy
pacman -S yaourt
pacman -S base-devel

Install couchdbkit
yaourt -S python2-couchdbkit

Setup the admin account for CouchDB
echo "admin = -pbkdf2-7a4cc99ded3299e01b97258f0d93eab6dfb0d23e,4a2a5b043eb60d06f3d0204939c35f96,10" >> /etc/couchdb/local.ini
systemctl restart couchdb

Copy the configuration file of ers
mkdir /etc/ers-node/
cp ers-node.ini /etc/ers-node/

Start ers
python2 ers/daemon.py --config /etc/ers-node/ers-node.ini

Configuration
=============

TBW

Usage
=====

TBW

License and Acknowledgements
============================

TBW

[![Build Status](https://travis-ci.org/ers-devs/ers.png?branch=master)](https://travis-ci.org/ers-devs/ers)
