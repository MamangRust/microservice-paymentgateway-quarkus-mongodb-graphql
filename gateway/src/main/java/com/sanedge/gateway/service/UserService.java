package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.UserDto.*;
import io.smallrye.mutiny.Uni;

public interface UserService {
    Uni<FindAllUserResponse> listUsers(int page, int size, String search);
    Uni<FindByIdUserResponse> getUser(String id);
    Uni<CreateUserResponse> createUser(CreateUserRequest body);
    Uni<UpdateUserResponse> updateUser(String id, UpdateUserRequest body);
    Uni<TrashedUserResponse> deleteUser(String id);
}
