To run & join:

    go run FileServer.go daemon.go MapleJuice.go main.go

To clean:

    bash clean.sh

While program running...

- To print membership list:

      member
    
- To leave the group:

      leave
    
- To display localhost id:

      id
    
- To display localhost ip:

      ip
      
- To display localhost hash value:
      
      hash
      
- To list file storage on current machine:

      store

- To list all files:

      all
      
- To print locations of a file:

      ls <filename>

- To insert / update a file:

      put <local_filename> <remote_filename>
      
- To get a file:

      get <remote_filename> <local_filename>
      
- To delete a file:

      delete <filename>