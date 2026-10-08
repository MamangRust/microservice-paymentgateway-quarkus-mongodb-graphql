package com.sanedge.gateway.service;

import com.sanedge.gateway.dto.RoleDto.*;
import io.smallrye.mutiny.Uni;

public interface RoleService {
    Uni<FindAllRoleResponse> listRoles(int page, int size, String search);
    Uni<FindByIdRoleResponse> getRole(String id);
    Uni<FindByIdRoleResponse> getRoleByName(String name);
    Uni<FindAllRoleResponse> findActiveRoles(int page, int size, String search);
    Uni<FindAllRoleResponse> findTrashedRoles(int page, int size, String search);
    Uni<FindAllRoleResponse> findRolesByUserId(String userId);
    Uni<CreateRoleResponse> createRole(CreateRoleRequest body);
    Uni<UpdateRoleResponse> updateRole(String id, UpdateRoleRequest body);
    Uni<SimpleStatusMessageResponse> deleteRolePermanent(String id);
    Uni<TrashedRoleResponse> deleteRole(String id);
    Uni<TrashedRoleResponse> trashRole(String id);
    Uni<TrashedRoleResponse> restoreRole(String id);
    Uni<SimpleStatusMessageResponse> restoreAllRoles();
    Uni<SimpleStatusMessageResponse> deleteAllRoles();
    Uni<AssignRoleToUserResponse> assignRoleToUser(String userId, String roleId);
    Uni<SimpleStatusMessageResponse> removeRoleFromUser(String userId, String roleId);
}
