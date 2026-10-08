package com.sanedge.common.adapter.role;

import java.util.List;

import com.sanedge.common.adapter.model.Role;
import com.sanedge.common.adapter.model.UserRole;

import io.smallrye.mutiny.Uni;

/**
 * User&harr;role surface: resolving a user's roles and (un)assigning them. Owns
 * the calls to the role service's user-role RPCs.
 */
public interface UserRolePort {

    Uni<List<Role>> findByUserId(String userId);

    Uni<UserRole> assignRoleToUser(String userId, String roleId);

    Uni<Void> removeRoleFromUser(String userId, String roleId);
}
